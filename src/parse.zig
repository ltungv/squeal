const std = @import("std");
const squeal_token = @import("token.zig");
const squeal_table = @import("table.zig");

// A parser for SQueaL.
pub const Parser = struct {
    tokenizer: squeal_token.Tokenizer,
    token_prev: ?squeal_token.Token,
    token_curr: ?squeal_token.Token,

    // Parse error.
    pub const Error = error{
        UnrecognizedCommand,
        UnexpectedToken,
    } ||
        squeal_table.Row.Error ||
        squeal_token.Tokenizer.Error ||
        std.fmt.ParseIntError;

    /// Create a new parser.
    pub fn new(src: []const u8) @This() {
        return .{
            .tokenizer = squeal_token.Tokenizer.new(src),
            .token_prev = null,
            .token_curr = null,
        };
    }

    /// Parse a statement.
    pub fn parse(this: *@This()) Error!Statement {
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
        if (std.mem.eql(u8, command_name.lex, "constants")) {
            return Statement{ .Command = CommandStatement.Constants };
        }
        if (std.mem.eql(u8, command_name.lex, "exit")) {
            return Statement{ .Command = CommandStatement.Exit };
        }
        return Error.UnrecognizedCommand;
    }

    fn statement(this: *@This()) Error!Statement {
        if (try this.match(.Select)) {
            return Statement{ .Query = .{ .Select = {} } };
        }
        if (try this.match(.Insert)) {
            const row_id = try this.integer();
            const row_key = try this.string();
            const row_val = try this.string();
            const row = try squeal_table.Row.new(row_id, row_key, row_val);
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

    fn integer(this: *@This()) Error!u32 {
        if (!try this.match(.Integer)) {
            return Error.UnexpectedToken;
        }
        return try std.fmt.parseInt(u32, this.token_prev.?.lex, 10);
    }

    // Go forward one token.
    fn advance(this: *@This()) Error!void {
        this.token_prev = this.token_curr;
        this.token_curr = try this.tokenizer.next();
    }

    fn match(this: *@This(), token_type: squeal_token.TokenType) Error!bool {
        const matched = this.check(token_type);
        if (matched) {
            try this.advance();
        }
        return matched;
    }

    fn check(this: *const @This(), token_type: squeal_token.TokenType) bool {
        if (this.token_curr) |token| {
            return token.typ == token_type;
        }
        return false;
    }
};

pub const StatementType = enum {
    Command,
    Query,
};

pub const Statement = union(StatementType) {
    Command: CommandStatement,
    Query: QueryStatement,
};

pub const CommandStatement = enum {
    BTree,
    Constants,
    Exit,
};

pub const QueryStatementType = enum {
    Insert,
    Select,
};

pub const QueryStatement = union(QueryStatementType) {
    Insert: InsertQuery,
    Select: void,
};

pub const InsertQuery = struct {
    row: squeal_table.Row,
};
