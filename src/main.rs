use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    mem,
};

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROW_SIZE: usize = mem::size_of::<Row>();
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

enum StatementType {
    Insert,
    Select,
}

#[derive(Debug, Clone, Copy)]
struct Row {
    id: u32,
    username: [u8; 32],
    email: [u8; 255],
}

struct Cursor<'a> {
    table: &'a mut Table,
    row_num: usize,
    end_of_table: bool,
}

struct Pager {
    file: File,
    pages: Vec<Option<Box<[u8; PAGE_SIZE]>>>,
    file_length: usize,
}

struct Table {
    pager: Pager,
    num_rows: usize,
}

struct Statement {
    statement_type: StatementType,
    row_to_insert: Option<Row>,
}

impl Row {
    fn new(id: u32, username: &str, email: &str) -> Result<Row, &'static str> {
        let mut row = Row {
            id,
            username: [0; 32],
            email: [0; 255],
        };

        if username.len() > 32 {
            return Err("Username too long");
        }
        if email.len() > 255 {
            return Err("Email too long");
        }

        row.username[..username.len()].copy_from_slice(username.as_bytes());
        row.email[..email.len()].copy_from_slice(email.as_bytes());

        Ok(row)
    }
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

    fn table_end(table: &'a mut Table) -> io::Result<Cursor<'a>> {
        let num_rows = table.num_rows;
        let cursor = Cursor {
            table,
            row_num: num_rows,
            end_of_table: true,
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

    fn value(&mut self) -> io::Result<Option<&mut Row>> {
        if self.end_of_table {
            Ok(None)
        } else {
            self.table.row_slot(self.row_num).map(Some)
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

    fn get_page(&mut self, page_num: usize) -> io::Result<&mut [u8; PAGE_SIZE]> {
        if page_num >= TABLE_MAX_PAGES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Tried to fetch page number out of bounds",
            ));
        }

        if self.pages[page_num].is_none() {
            let mut page = Box::new([0; PAGE_SIZE]);

            let num_pages = (self.file_length as f64 / PAGE_SIZE as f64).ceil() as usize;

            if page_num < num_pages {
                println!("Reading page {} from file", page_num);
                self.file
                    .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))?;
                let bytes_read = self.file.read(&mut page[..])?;

                if bytes_read < PAGE_SIZE && page_num == num_pages - 1 {
                    // This is fine, we've read a partial last page
                    println!("Read partial page: {} bytes", bytes_read);
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
    fn new(filename: &str) -> io::Result<Table> {
        let pager: Pager = Pager::new(filename)?;
        let num_rows: usize = pager.file_length / ROW_SIZE;
        Ok(Table { pager, num_rows })
    }

    fn close(&mut self) -> io::Result<()> {
        let full_pages: usize = self.num_rows / ROWS_PER_PAGE;
        for i in 0..full_pages {
            if self.pager.pages[i].is_some() {
                self.pager.flush(i, PAGE_SIZE)?;
            }
        }

        let additional_rows = self.num_rows % ROWS_PER_PAGE;
        if additional_rows > 0 {
            let page_num = full_pages;
            if self.pager.pages[page_num].is_some() {
                self.pager.flush(page_num, additional_rows * ROW_SIZE)?;
            }
        }

        println!("Closed table with {} rows", self.num_rows);
        Ok(())
    }

    fn row_slot(&mut self, row_num: usize) -> io::Result<&mut Row> {
        let page_num = row_num / ROWS_PER_PAGE;
        let page = self.pager.get_page(page_num)?;
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        Ok(unsafe { &mut *(page[byte_offset..].as_mut_ptr() as *mut Row) })
    }

    fn insert(&mut self, row: Row) -> io::Result<()> {
        if self.num_rows >= TABLE_MAX_ROWS {
            return Err(io::Error::new(io::ErrorKind::Other, "Error: Table full."));
        }

        let row_num = self.num_rows;
        let page_num = row_num / ROWS_PER_PAGE;
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        let page = self.pager.get_page(page_num)?;
        unsafe {
            std::ptr::write(page[byte_offset..].as_mut_ptr() as *mut Row, row);
        }

        self.num_rows += 1;

        // Flush only the inserted row
        self.pager.flush(page_num, byte_offset + ROW_SIZE)?;

        println!("Inserted row at index {}", row_num);

        Ok(())
    }
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Must supply a database filename.");
        return Ok(());
    }
    let filename = &args[1];
    let mut table = Table::new(filename)?;

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

// New: Function to prepare statements
fn prepare_statement(input: &str) -> Result<Statement, &'static str> {
    if input.starts_with("insert") {
        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.len() != 4 {
            return Err("Syntax error. Use: insert ID USERNAME EMAIL");
        }
        let id: u32 = parts[1].parse().map_err(|_| "Invalid ID")?;
        let row = Row::new(id, parts[2], parts[3])?;
        Ok(Statement {
            statement_type: StatementType::Insert,
            row_to_insert: Some(row),
        })
    } else if input.starts_with("select") {
        Ok(Statement {
            statement_type: StatementType::Select,
            row_to_insert: None,
        })
    } else {
        Err("Unrecognized keyword at start of statement")
    }
}

// New: Function to execute statements
fn execute_statement(statement: &Statement, table: &mut Table) -> io::Result<()> {
    match statement.statement_type {
        StatementType::Insert => match &statement.row_to_insert {
            Some(row) => match table.insert(*row) {
                Ok(()) => {
                    println!("Inserted");
                }
                Err(e) => println!("Error inserting row: {}", e),
            },
            None => println!("Error: No row to insert."),
        },
        StatementType::Select => {
            let mut cursor = Cursor::table_start(table)?;

            while !cursor.end_of_table {
                if let Some(row) = cursor.value()? {
                    println!(
                        "({}, {}, {})",
                        row.id,
                        std::str::from_utf8(&row.username)
                            .unwrap()
                            .trim_end_matches('\0'),
                        std::str::from_utf8(&row.email)
                            .unwrap()
                            .trim_end_matches('\0')
                    );
                }
                cursor.advance()?;
            }
            println!("Executed.");
        }
    }
    Ok(())
}
