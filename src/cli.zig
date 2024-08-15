const std = @import("std");
const assert = @import("assert.zig");
const squeal = @import("squeal.zig");

const PAGE_SIZE = 32 * 1024;
const PAGE_COUNT = 1024 * 1024;

/// The tokenizer for our Squeal.
const Tokenizer = struct {
    src: []const u8,
    lexeme_head: usize,
    lexeme_tail: usize,

    const Error = error{UnexpectedByte};

    /// Initialize a new Squeal tokenizer.
    fn new(src: []const u8) @This() {
        return .{
            .src = src,
            .lexeme_head = 0,
            .lexeme_tail = 0,
        };
    }

    /// Returns the next token in the source code.
    fn next(this: *@This()) Error!?Token {
        this.skipWhitespace();
        const byte = this.advance() orelse return null;
        if (byte == '.') {
            return this.makeToken(.Dot);
        }
        if (byte == '\'') {
            return try this.string();
        }
        if (byte == '-' or std.ascii.isDigit(byte)) {
            return try this.integer();
        }
        if (std.ascii.isAlphabetic(byte)) {
            return try this.identity();
        }
        return Error.UnexpectedByte;
    }

    fn isSingleQuote(byte: u8) bool {
        return byte == '\'';
    }

    fn string(this: *@This()) Error!Token {
        while (!this.peekCheck(isSingleQuote)) {
            _ = this.advance();
        }
        try this.consume('\'');
        return this.makeToken(.String);
    }

    fn identity(this: *@This()) Error!Token {
        while (this.peekCheck(std.ascii.isAlphabetic)) {
            _ = this.advance();
        }
        var typ = TokenType.Ident;
        const lex = this.src[this.lexeme_head..this.lexeme_tail];
        if (std.mem.eql(u8, lex, "count")) {
            typ = .Count;
        }
        if (std.mem.eql(u8, lex, "select")) {
            typ = .Select;
        }
        if (std.mem.eql(u8, lex, "insert")) {
            typ = .Insert;
        }
        if (std.mem.eql(u8, lex, "null")) {
            typ = .Null;
        }
        return this.makeToken(typ);
    }

    fn integer(this: *@This()) Error!Token {
        while (this.peekCheck(std.ascii.isDigit)) {
            _ = this.advance();
        }
        return this.makeToken(.Integer);
    }

    fn makeToken(this: *const @This(), typ: TokenType) Token {
        const lex = this.src[this.lexeme_head..this.lexeme_tail];
        return Token{ .typ = typ, .lex = lex };
    }

    fn skipWhitespace(this: *@This()) void {
        while (this.peek()) |byte| {
            if (byte == ' ' or byte == '\r' or byte == '\t' or byte == '\n') {
                _ = this.advance();
            } else {
                break;
            }
        }
        this.lexeme_head = this.lexeme_tail;
    }

    fn advance(this: *@This()) ?u8 {
        const byte = this.peek() orelse return null;
        this.lexeme_tail += 1;
        return byte;
    }

    fn consume(this: *@This(), expected: u8) Error!void {
        const byte = this.advance() orelse return Error.UnexpectedByte;
        if (byte != expected) {
            return Error.UnexpectedByte;
        }
    }

    fn peek(this: *const @This()) ?u8 {
        if (this.lexeme_tail >= this.src.len) {
            return null;
        }
        return this.src[this.lexeme_tail];
    }

    fn peekNext(this: *const @This()) ?u8 {
        if (this.lexeme_tail + 1 >= this.src.len) {
            return null;
        }
        return this.src[this.lexeme_tail + 1];
    }

    fn peekCheck(this: *const @This(), comptime predicate_fn: fn (u8) bool) bool {
        const byte = this.peek() orelse return false;
        return predicate_fn(byte);
    }
};

/// A token in our Squeal.
const Token = struct {
    typ: TokenType,
    lex: []const u8,
};

/// All types of tokens in our Squeal.
const TokenType = enum {
    Dot,
    Count,
    Select,
    Insert,
    Ident,
    Integer,
    String,
    Null,
};

/// A parser for Squeal.
const Parser = struct {
    tokenizer: Tokenizer,
    token_prev: ?Token,
    token_curr: ?Token,

    /// Parse error.
    const Error = error{ UnrecognizedCommand, UnexpectedToken } ||
        squeal.Row.Error ||
        Tokenizer.Error ||
        std.fmt.ParseIntError;

    /// Create a new parser.
    fn new(src: []const u8) @This() {
        return .{
            .tokenizer = Tokenizer.new(src),
            .token_prev = null,
            .token_curr = null,
        };
    }

    /// Parse a statement.
    fn parse(this: *@This()) Error!Statement {
        try this.advance();
        if (try this.match(.Dot)) {
            return this.metaCommand();
        }
        return this.statement();
    }

    fn metaCommand(this: *@This()) Error!Statement {
        if (!try this.match(.Ident)) {
            return Error.UnexpectedToken;
        }
        const command_name = this.token_prev orelse unreachable;
        if (std.mem.eql(u8, command_name.lex, "btree")) {
            return Statement{ .Command = CommandStatement.BTree };
        }
        if (std.mem.eql(u8, command_name.lex, "exit")) {
            return Statement{ .Command = CommandStatement.Exit };
        }
        return Error.UnrecognizedCommand;
    }

    fn statement(this: *@This()) Error!Statement {
        if (try this.match(.Count)) {
            return Statement{ .Query = .{ .Count = {} } };
        }
        if (try this.match(.Select)) {
            return Statement{ .Query = .{ .Select = {} } };
        }
        if (try this.match(.Insert)) {
            const row_id = try this.integer();
            const row_key = try this.string();
            const row_val = try this.string();
            const row = try squeal.Row.new(row_id, row_key, row_val);
            return Statement{ .Query = .{ .Insert = .{ .row = row } } };
        }
        return Error.UnrecognizedCommand;
    }

    fn string(this: *@This()) Error![]const u8 {
        if (!try this.match(.String)) {
            return Error.UnexpectedToken;
        }
        const s = this.token_prev.?.lex;
        return s[1 .. s.len - 1];
    }

    fn integer(this: *@This()) Error!u64 {
        if (!try this.match(.Integer)) {
            return Error.UnexpectedToken;
        }
        return try std.fmt.parseInt(u64, this.token_prev.?.lex, 10);
    }

    // Go forward one token.
    fn advance(this: *@This()) Error!void {
        this.token_prev = this.token_curr;
        this.token_curr = try this.tokenizer.next();
    }

    fn match(this: *@This(), token_type: TokenType) Error!bool {
        const matched = this.check(token_type);
        if (matched) {
            try this.advance();
        }
        return matched;
    }

    fn check(this: *const @This(), token_type: TokenType) bool {
        if (this.token_curr) |token| {
            return token.typ == token_type;
        }
        return false;
    }
};

/// All types of statements.
const StatementType = enum {
    Command,
    Query,
};

/// A statement.
const Statement = union(StatementType) {
    Command: CommandStatement,
    Query: QueryStatement,
};

/// A command statement.
const CommandStatement = enum {
    BTree,
    Exit,
};

/// All types of query statements.
const QueryStatementType = enum {
    Count,
    Select,
    Insert,
};

/// A query statement.
const QueryStatement = union(QueryStatementType) {
    Count: void,
    Select: void,
    Insert: InsertQuery,
};

/// An insert query.
const InsertQuery = struct {
    row: squeal.Row,
};

// The virtual machine that executes SQL statements.
pub const Vm = struct {
    allocator: std.mem.Allocator,
    stream: *const Stream,
    table: *squeal.Table,

    /// VM's error.
    const Error = Stream.ReadError || Stream.WriteError || squeal.Table.Error;

    /// Create a new VM.
    pub fn init(
        allocator: std.mem.Allocator,
        stream: *const Stream,
        table: *squeal.Table,
    ) Error!@This() {
        return .{
            .allocator = allocator,
            .stream = stream,
            .table = table,
        };
    }

    /// Run the VM which repeatedly reads a line from the stream, parses it, and executes it.
    pub fn run(this: *@This()) Error!void {
        var finished = false;
        while (!finished) {
            try this.stream.print("db > ");

            var line_buf: [PAGE_SIZE]u8 = undefined;
            const line = this.stream.readln(&line_buf) catch |err| {
                try this.stream.eprint(err);
                continue;
            } orelse {
                continue;
            };

            var parser = Parser.new(line);
            const statement = parser.parse() catch |err| {
                try this.stream.eprint(err);
                continue;
            };

            finished = this.exec(&statement) catch |err| {
                try this.stream.eprint(err);
                continue;
            };
        }
    }

    fn exec(this: *@This(), statement: *const Statement) Error!bool {
        switch (statement.*) {
            .Command => |command| switch (command) {
                .BTree => {
                    try this.stream.print("Tree:\n");
                    try this.printTree(0, 0);
                },
                .Exit => return true,
            },
            .Query => |query| {
                switch (query) {
                    .Count => {
                        const count = try this.table.count();
                        try this.stream.printf("{d}\n", .{count});
                    },
                    .Select => {
                        var cursor = try this.table.find(0, 0);
                        while (!cursor.done) {
                            const value = try cursor.value();
                            var stream = std.io.fixedBufferStream(value);
                            var reader = stream.reader();
                            const row = try reader.readStructEndian(squeal.Row, .little);
                            const key = row.key_buf[0..row.key_len];
                            const val = row.val_buf[0..row.val_len];
                            try this.stream.printf("({d}, {s}, {s})\n", .{ row.id, key, val });
                            try cursor.advance();
                        }
                    },
                    .Insert => |q| {
                        var value: [@sizeOf(squeal.Row)]u8 = undefined;
                        var stream = std.io.fixedBufferStream(&value);
                        var writer = stream.writer();
                        try writer.writeStructEndian(q.row, .little);
                        try this.table.insert(q.row.id, &value);
                    },
                }
                try this.stream.print("Executed.\n");
            },
        }
        return false;
    }

    fn printIndentation(this: *@This(), level: usize) Error!void {
        var i = level;
        while (i > 0) : (i -= 1) {
            try this.stream.print("  ");
        }
    }

    fn printTree(this: *@This(), page: u32, indentation: usize) Error!void {
        const node = try this.table.pager.get(page);
        switch (squeal.getNodeType(node)) {
            squeal.LEAF_NODE_TYPE => {
                const cell_count = squeal.getLeafCellCount(node);
                try this.printIndentation(indentation);
                try this.stream.printf("- leaf (size {d})\n", .{cell_count});
                var cell: u32 = 0;
                while (cell < cell_count) : (cell += 1) {
                    const cell_key = squeal.getLeafCellKey(node, this.table.leaf_cell_size, cell);
                    try this.printIndentation(indentation + 1);
                    try this.stream.printf("  - {d}\n", .{cell_key});
                }
            },
            squeal.INNER_NODE_TYPE => {
                const cell_count = squeal.getInnerCellCount(node);
                const right_ptr = squeal.getInnerRightPointer(node);
                try this.printIndentation(indentation);
                try this.stream.printf("- internal (size {d})\n", .{cell_count});

                var keys = try std.ArrayList(u64).initCapacity(this.allocator, cell_count);
                defer keys.deinit();

                var ptrs = try std.ArrayList(u32).initCapacity(this.allocator, cell_count);
                defer ptrs.deinit();

                var cell: u32 = 0;
                while (cell < cell_count) : (cell += 1) {
                    try keys.append(squeal.getInnerCellKey(node, cell));
                    try ptrs.append(squeal.getInnerCellPointer(node, cell));
                }

                for (keys.items, ptrs.items) |key, ptr| {
                    try this.printTree(ptr, indentation + 1);
                    try this.printIndentation(indentation + 1);
                    try this.stream.printf("- key {d}\n", .{key});
                }
                try this.printTree(right_ptr, indentation + 1);
            },
            else => return Error.Corrupted,
        }
    }
};

/// A wrapper around a pair of StreamSource.Reader and StreamSource.Writer.
pub const Stream = struct {
    reader: std.io.StreamSource.Reader,
    writer: std.io.StreamSource.Writer,

    /// Error that occurs when writing to the stream.
    const WriteError = std.io.StreamSource.WriteError;

    /// Error that occurs when reading from the stream.
    const ReadError = error{ StreamTooLong, NoSpaceLeft } || std.io.StreamSource.ReadError;

    /// Print a simple string.
    fn print(this: *const @This(), comptime format: []const u8) WriteError!void {
        try this.writer.print(format, .{});
    }

    /// Print a formatted string.
    fn printf(this: *const @This(), comptime format: []const u8, args: anytype) WriteError!void {
        try this.writer.print(format, args);
    }

    /// Print an error in its default format.
    fn eprint(this: *const @This(), err: anyerror) WriteError!void {
        try this.writer.print("{!}\n", .{err});
    }

    /// Read a single line from the stream into an owned slice.
    fn readln(this: *const @This(), buf: []u8) ReadError!?[]u8 {
        return this.reader.readUntilDelimiterOrEof(buf, '\n');
    }
};

fn makeLines(comptime lines: []const []const u8) []const u8 {
    if (lines.len == 0) return "";
    comptime var result = lines[0];
    comptime for (lines[1..]) |line| {
        result = result ++ "\n" ++ line;
    };
    return result;
}

test "vm run .exit" {
    const filepath = try assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

    const input = makeLines(&[_][]const u8{".exit"});
    const output = makeLines(&[_][]const u8{"db > "});
    try assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}

test "vm run empty line" {
    const filepath = try assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

    const input = makeLines(&[_][]const u8{
        "",
        ".exit",
    });
    const output = makeLines(&[_][]const u8{
        "db > error.UnrecognizedCommand",
        "db > ",
    });
    try assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}

test "vm run single insert then select" {
    const filepath = try assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

    const input = makeLines(&[_][]const u8{
        "insert 1 'key' 'value'",
        "select",
        ".exit",
    });
    const output = makeLines(&[_][]const u8{
        "db > Executed.",
        "db > (1, key, value)",
        "Executed.",
        "db > ",
    });
    try assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}

test "vm run max key and value size insert" {
    const filepath = try assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

    comptime var max_size_key: [squeal.Row.MAX_KEY_LEN]u8 = undefined;
    @memset(&max_size_key, 'a');
    comptime var max_size_val: [squeal.Row.MAX_VAL_LEN]u8 = undefined;
    @memset(&max_size_val, 'a');

    const input = makeLines(&[_][]const u8{
        std.fmt.comptimePrint("insert 1 '{s}' '{s}'", .{ max_size_key, max_size_val }),
        ".exit",
    });
    const output = makeLines(&[_][]const u8{
        "db > Executed.",
        "db > ",
    });
    try assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}

test "vm run long key insert" {
    const filepath = try assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

    comptime var long_key: [squeal.Row.MAX_KEY_LEN + 1]u8 = undefined;
    @memset(&long_key, 'a');

    const input = makeLines(&[_][]const u8{
        std.fmt.comptimePrint("insert 1 '{s}' 'value'", .{long_key}),
        ".exit",
    });
    const output = makeLines(&[_][]const u8{
        "db > error.KeyTooLong",
        "db > ",
    });
    try assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}

test "vm run long value insert" {
    const filepath = try assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

    comptime var long_value: [squeal.Row.MAX_VAL_LEN + 1]u8 = undefined;
    @memset(&long_value, 'a');

    const input = makeLines(&[_][]const u8{
        std.fmt.comptimePrint("insert 1 'key' '{s}'", .{long_value}),
        ".exit",
    });
    const output = makeLines(&[_][]const u8{
        "db > error.ValueTooLong",
        "db > ",
    });
    try assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}

test "vm keeps data on reopen after closing" {
    const filepath = try assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);
    {
        const input = makeLines(&[_][]const u8{
            "insert 0 'key0' 'value0'",
            "insert 1 'key1' 'value1'",
            "insert 2 'key2' 'value2'",
            ".exit",
        });
        const output = makeLines(&[_][]const u8{
            "db > Executed.",
            "db > Executed.",
            "db > Executed.",
            "db > ",
        });
        try assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
    }
    {
        const input = makeLines(&[_][]const u8{
            "select",
            ".exit",
        });
        const output = makeLines(&[_][]const u8{
            "db > (0, key0, value0)",
            "(1, key1, value1)",
            "(2, key2, value2)",
            "Executed.",
            "db > ",
        });
        try assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
    }
}

test "vm allows printing one-node btree" {
    const filepath = try assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

    const input = makeLines(&[_][]const u8{
        "insert 3 'key' 'value'",
        "insert 1 'key' 'value'",
        "insert 2 'key' 'value'",
        ".btree",
        ".exit",
    });
    const output = makeLines(&[_][]const u8{
        "db > Executed.",
        "db > Executed.",
        "db > Executed.",
        "db > Tree:",
        "- leaf (size 3)",
        "    - 1",
        "    - 2",
        "    - 3",
        "db > ",
    });
    try assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}

test "vm shows error when inserting row with duplicate id" {
    const filepath = try assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

    const input = makeLines(&[_][]const u8{
        "insert 0 'hello' 'world'",
        "insert 0 'hello' 'world'",
        ".exit",
    });
    const output = makeLines(&[_][]const u8{
        "db > Executed.",
        "db > error.DuplicateKey",
        "db > ",
    });
    try assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}
