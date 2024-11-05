use std::collections::HashMap;

pub enum StatementType {
    Insert,
    Select,
}

#[derive(Clone, Debug)]
pub enum Value {
    Integer(i64),
    Text(String),
}

#[derive(Clone)]
pub struct Row {
    pub values: HashMap<String, Value>,
}

pub struct Statement {
    pub statement_type: StatementType,
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<Value>,
    pub where_clause: Option<WhereClause>,
}

pub struct WhereClause {
    pub column: String,
    pub operator: String,
    pub value: Value,
}

impl Row {
    pub fn new() -> Row {
        Row {
            values: HashMap::new(),
        }
    }
}

pub fn prepare_statement(input: &str) -> Result<Statement, String> {
    let tokens = tokenize(input);
    println!("Tokens: {:?}", tokens);

    match tokens.get(0).map(|s| s.to_lowercase()).as_deref() {
        Some("insert") => parse_insert(&tokens),
        Some("select") => parse_select(&tokens),
        _ => Err("Unrecognized keyword at start of statement".to_string()),
    }
}

fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current_token = String::new();
    let mut in_quotes = false;

    for ch in input.chars() {
        match ch {
            '\'' => {
                current_token.push(ch);
                in_quotes = !in_quotes;
            }
            ' ' | ',' | '(' | ')' if !in_quotes => {
                if !current_token.is_empty() {
                    tokens.push(current_token);
                    current_token = String::new();
                }
                if ch != ' ' {
                    tokens.push(ch.to_string());
                }
            }
            _ => current_token.push(ch),
        }
    }

    if !current_token.is_empty() {
        tokens.push(current_token);
    }

    tokens
}

fn parse_insert(tokens: &[String]) -> Result<Statement, String> {
    println!("Parsing INSERT statement: {:?}", tokens);

    if tokens.len() < 7 || tokens[1].to_lowercase() != "into" {
        return Err("Invalid Insert Statement".to_string());
    }

    let table_name = tokens[2].clone();
    let mut i = 3;

    // Ensure the next token is an opening parenthesis
    if tokens[i] != "(" {
        return Err("Expected '(' after table name".to_string());
    }
    i += 1;

    // Parse columns
    let mut columns = Vec::new();
    while i < tokens.len() && tokens[i] != ")" {
        if tokens[i] != "," {
            columns.push(tokens[i].clone());
        }
        i += 1;
    }

    // Ensure we found the closing parenthesis
    if i >= tokens.len() || tokens[i] != ")" {
        return Err("Expected ')' after columns".to_string());
    }
    i += 1;

    // Check for VALUES keyword
    if i >= tokens.len() || tokens[i].to_lowercase() != "values" {
        return Err("Expected 'VALUES' keyword".to_string());
    }
    i += 1;

    // Ensure the next token is an opening parenthesis
    if i >= tokens.len() || tokens[i] != "(" {
        return Err("Expected '(' after VALUES".to_string());
    }
    i += 1;

    // Parse values
    let mut value_strings = Vec::new();
    while i < tokens.len() && tokens[i] != ")" {
        if tokens[i] != "," {
            value_strings.push(tokens[i].clone());
        }
        i += 1;
    }

    // Ensure we found the closing parenthesis
    if i >= tokens.len() || tokens[i] != ")" {
        return Err("Expected ')' after values".to_string());
    }

    let values = value_strings
        .iter()
        .map(|s| parse_value(s))
        .collect::<Result<Vec<Value>, String>>()?;

    if columns.len() != values.len() {
        return Err(format!(
            "Number of columns ({}) doesn't match number of values ({})",
            columns.len(),
            values.len()
        ));
    }

    Ok(Statement {
        statement_type: StatementType::Insert,
        table_name,
        columns,
        values,
        where_clause: None,
    })
}

fn parse_select(tokens: &[String]) -> Result<Statement, String> {
    if tokens.len() < 4 || tokens[tokens.len() - 2].to_lowercase() != "from" {
        return Err("Invalid SELECT syntax".to_string());
    }

    let columns = if tokens[1] == "*" {
        vec!["*".to_string()]
    } else {
        tokens[1..tokens.len() - 2]
            .iter()
            .filter(|&s| s != ",")
            .map(|s| s.to_string())
            .collect()
    };

    let table_name = tokens[tokens.len() - 1].to_string();

    // We're not handling WHERE clauses for now, but you can add that later

    Ok(Statement {
        statement_type: StatementType::Select,
        table_name,
        columns,
        values: vec![],
        where_clause: None,
    })
}

//helper functions

fn parse_parentheses_list(tokens: &[&str]) -> Result<Vec<String>, String> {
    let mut result = vec![];
    let mut current_item = String::new();
    let mut depth = 0;
    let mut in_quotes = false;

    for token in tokens {
        if token.starts_with('(') && !in_quotes {
            depth += 1;
            if depth == 1 {
                continue; // Skip the opening parenthesis of the outermost level
            }
        }

        if depth == 0 && !token.starts_with('(') {
            break; // We've reached the end of the parentheses list
        }

        // Handle quotes
        if token.starts_with('\'') {
            in_quotes = true;
        }
        if token.ends_with('\'') && !token.ends_with("\'\'") {
            in_quotes = false;
        }

        // Remove leading/trailing parentheses and commas, but only if not in quotes
        let cleaned_token = if !in_quotes {
            token.trim_matches(|c| c == '(' || c == ')' || c == ',')
        } else {
            token
        };

        if !cleaned_token.is_empty() {
            if !current_item.is_empty() && !in_quotes {
                current_item.push(' ');
            }
            current_item.push_str(cleaned_token);
        }

        if (token.ends_with(',') || token.ends_with(')')) && !in_quotes {
            result.push(current_item.trim().to_string());
            current_item.clear();
        }

        if token.ends_with(')') && !in_quotes {
            depth -= 1;
            if depth == 0 {
                break; // We've reached the end of the list
            }
        }
    }

    if depth != 0 {
        return Err("Mismatched parentheses".to_string());
    }

    if !current_item.is_empty() {
        result.push(current_item.trim().to_string());
    }

    Ok(result)
}

fn parse_value(s: &str) -> Result<Value, String> {
    if s.starts_with('\'') && s.ends_with('\'') {
        Ok(Value::Text(s.trim_matches('\'').to_string()))
    } else if let Ok(num) = s.parse::<i64>() {
        Ok(Value::Integer(num))
    } else {
        Err(format!("Invalid value: {}", s))
    }
}
