mod sql_parser;

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    mem,
};

use sql_parser::{prepare_statement, Row, Statement, StatementType, Value, WhereClause};

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;

struct Cursor<'a> {
    table: &'a mut Table,
    row_num: usize,
    end_of_table: bool,
}

struct Pager {
    file: File,
    pages: Vec<Option<Vec<u8>>>,
    file_length: usize,
}

struct Table {
    pager: Pager,
    num_rows: usize,
    columns: Vec<String>,
}

impl<'a> Cursor<'a> {
    fn table_start(table: &'a mut Table) -> io::Result<Cursor<'a>> {
        let end_of_table = table.num_rows == 0;
        let cursor = Cursor {
            table,
            row_num: 0,
            end_of_table,
        };
        Ok(cursor)
    }

    fn advance(&mut self) -> io::Result<()> {
        self.row_num += 1;

        if self.row_num >= self.table.num_rows {
            self.end_of_table = true;
        }

        Ok(())
    }

    fn value(&mut self) -> io::Result<Option<Row>> {
        if self.end_of_table {
            Ok(None)
        } else {
            self.table.row_slot(self.row_num)
        }
    }
}

impl Pager {
    fn new(filename: &str) -> io::Result<Pager> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)?;

        let file_length = file.metadata()?.len() as usize;

        Ok(Pager {
            file,
            pages: vec![None; TABLE_MAX_PAGES],
            file_length,
        })
    }

    fn flush(&mut self, page_num: usize, size: usize) -> io::Result<()> {
        if let Some(page) = &self.pages[page_num] {
            println!("Flushing page {} with size {}", page_num, size);
            self.file
                .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))?;
            self.file.write_all(&page[..size])?;
            self.file.flush()?;

            // Update file_length if necessary
            let end_of_write = ((page_num * PAGE_SIZE) + size) as u64;
            if end_of_write > self.file_length as u64 {
                self.file_length = end_of_write as usize;
                self.file.set_len(end_of_write)?;
            }
        }
        Ok(())
    }

    fn get_page(&mut self, page_num: usize) -> io::Result<&mut Vec<u8>> {
        if page_num >= TABLE_MAX_PAGES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Tried to fetch page number out of bounds",
            ));
        }

        if self.pages[page_num].is_none() {
            let mut page = vec![0; PAGE_SIZE];

            let num_pages = (self.file_length as f64 / PAGE_SIZE as f64).ceil() as usize;

            if page_num < num_pages {
                println!("Reading page {} from file", page_num);
                self.file
                    .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))?;
                let bytes_read = self.file.read(&mut page[..])?;

                if bytes_read < PAGE_SIZE && page_num == num_pages - 1 {
                    page.truncate(bytes_read);
                } else if bytes_read < PAGE_SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Failed to read full page",
                    ));
                }
            } else {
                println!("Initializing new page {}", page_num);
            }

            self.pages[page_num] = Some(page);
        }

        Ok(self.pages[page_num].as_mut().unwrap())
    }
}

impl Table {
    fn new(filename: &str, columns: Vec<String>) -> io::Result<Table> {
        let pager: Pager = Pager::new(filename)?;
        let num_rows: usize = pager.file_length / Self::row_size(&columns);
        Ok(Table {
            pager,
            num_rows,
            columns,
        })
    }

    fn row_size(columns: &[String]) -> usize {
        columns.len() * std::mem::size_of::<Value>()
    }

    fn close(&mut self) -> io::Result<()> {
        let row_size = Self::row_size(&self.columns);
        let full_pages = self.num_rows * row_size / PAGE_SIZE;
        for i in 0..full_pages {
            if self.pager.pages[i].is_some() {
                self.pager.flush(i, PAGE_SIZE)?;
            }
        }

        let additional_rows = self.num_rows % (PAGE_SIZE / row_size);
        if additional_rows > 0 {
            let page_num = full_pages;
            if self.pager.pages[page_num].is_some() {
                self.pager.flush(page_num, additional_rows * row_size)?;
            }
        }

        Ok(())
    }

    fn row_slot(&mut self, row_num: usize) -> io::Result<Option<Row>> {
        let row_size = Self::row_size(&self.columns);
        let page_num = row_num * row_size / PAGE_SIZE;
        let row_offset = row_num % (PAGE_SIZE / row_size);
        let byte_offset = row_offset * row_size;

        let page = self.pager.get_page(page_num)?;
        if byte_offset >= page.len() {
            return Ok(None);
        }

        let mut row = Row::new();
        for (i, column) in self.columns.iter().enumerate() {
            let value_offset = byte_offset + i * std::mem::size_of::<Value>();
            let value = Self::deserialize_value(&page[value_offset..]);
            row.values.insert(column.clone(), value);
        }

        Ok(Some(row))
    }

    fn insert(&mut self, row: Row) -> io::Result<()> {
        let row_size = self.serialize_row(&row).len();
        let page_num = self.num_rows * row_size / PAGE_SIZE;
        let row_offset = self.num_rows % (PAGE_SIZE / row_size);
        let byte_offset = row_offset * row_size;

        let page = self.pager.get_page(page_num)?;
        if byte_offset + row_size > page.len() {
            page.resize(byte_offset + row_size, 0);
        }

        let serialized_row = self.serialize_row(&row);
        let page = self.pager.get_page(page_num)?;
        page[byte_offset..byte_offset + row_size].copy_from_slice(&serialized_row);

        self.num_rows += 1;
        self.pager.flush(page_num, byte_offset + row_size)?;

        Ok(())
    }

    fn serialize_row(&self, row: &Row) -> Vec<u8> {
        let mut buffer = Vec::new();
        for column in &self.columns {
            match row.values.get(column) {
                Some(Value::Integer(i)) => buffer.extend_from_slice(&i.to_le_bytes()),
                Some(Value::Text(s)) => {
                    buffer.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    buffer.extend_from_slice(s.as_bytes());
                }
                None => buffer.extend_from_slice(&[0; 8]), // Default to 8 bytes of zeros for missing values
            }
        }
        buffer
    }

    fn deserialize_value(buffer: &[u8]) -> Value {
        if buffer.len() >= 8 {
            Value::Integer(i64::from_le_bytes([
                buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6],
                buffer[7],
            ]))
        } else {
            let len = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
            Value::Text(String::from_utf8_lossy(&buffer[4..4 + len]).to_string())
        }
    }
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Must supply a database filename.");
        return Ok(());
    }
    let filename = &args[1];

    let columns = vec![
        "id".to_string(),
        "username".to_string(),
        "email".to_string(),
    ];
    let mut table = Table::new(filename, columns)?;

    loop {
        print_prompt();

        let input = read_input();

        // New: Handle meta commands
        if input.starts_with('.') {
            match do_meta_command(&input, &mut table) {
                Ok(()) => continue,
                Err(err) => {
                    println!("Error executing meta command: {}", err);
                    continue;
                }
            }
        }
        match prepare_statement(&input) {
            Ok(statement) => {
                if let Err(error) = execute_statement(&statement, &mut table) {
                    println!("Error executing statement: {}", error);
                }
            }
            Err(error) => println!("Error: {}", error),
        }
    }
}

fn print_prompt() {
    print!("db > ");
    io::stdout().flush().unwrap();
}

fn read_input() -> String {
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .expect("Failed to read line");
    input.trim().to_string()
}

fn do_meta_command(input: &str, table: &mut Table) -> io::Result<()> {
    match input {
        ".exit" => {
            table.close()?;
            std::process::exit(0);
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Unrecognized command",
        )),
    }
}

// New: Function to execute statements
fn execute_statement(statement: &Statement, table: &mut Table) -> io::Result<()> {
    match statement.statement_type {
        StatementType::Insert => {
            let mut row = Row::new();
            for (column, value) in statement.columns.iter().zip(statement.values.iter()) {
                row.values.insert(column.clone(), value.clone());
            }
            match table.insert(row) {
                Ok(()) => println!("Inserted"),
                Err(e) => println!("Error inserting row: {}", e),
            }
        }
        StatementType::Select => {
            let mut cursor = Cursor::table_start(table)?;
            while !cursor.end_of_table {
                if let Some(row) = cursor.value()? {
                    print_row(&row, &statement.columns);
                }
                cursor.advance()?;
            }
            println!("Executed.");
        }
    }
    Ok(())
}

fn matches_where_clause(row: &Row, where_clause: &Option<WhereClause>) -> bool {
    match where_clause {
        Some(clause) => {
            if let Some(value) = row.values.get(&clause.column) {
                match (&clause.operator[..], &clause.value) {
                    ("=", Value::Integer(i)) => {
                        if let Value::Integer(row_i) = value {
                            row_i == i
                        } else {
                            false
                        }
                    }
                    ("=", Value::Text(s)) => {
                        if let Value::Text(row_s) = value {
                            row_s == s
                        } else {
                            false
                        }
                    }
                    // Add more operators as needed
                    _ => false,
                }
            } else {
                false
            }
        }
        None => true,
    }
}

fn print_row(row: &Row, columns: &[String]) {
    let values: Vec<String> = if columns[0] == "*" {
        row.values.iter().map(|(_, v)| value_to_string(v)).collect()
    } else {
        columns
            .iter()
            .map(|col| {
                row.values
                    .get(col)
                    .map(|v| value_to_string(v))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect()
    };
    println!("({})", values.join(", "));
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Integer(i) => i.to_string(),
        Value::Text(s) => format!("'{}'", s),
    }
}
