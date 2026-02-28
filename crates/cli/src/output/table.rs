use comfy_table::{Cell, Color, Table};

pub fn new_table(columns: &[&str]) -> Table {
    let mut table = Table::new();
    table.set_header(columns.iter().map(|c| Cell::new(c).fg(Color::White)));
    table.load_preset(comfy_table::presets::UTF8_FULL_CONDENSED);
    table
}
