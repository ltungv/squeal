const std = @import("std");
const ParseIntError = std.fmt.ParseIntError;
const Tokenizer = @import("token.zig").Tokenizer;
const Token = @import("token.zig").Token;
const TokenType = @import("token.zig").TokenType;
const Row = @import("table.zig").Row;

pub const Parser = struct {
    tokenizer: Tokenizer,
    token_prev: ?Token,
    token_curr: ?Token,

    pub const Error = error{
        UnrecognizedCommand,
        UnexpectedToken,
    } || Row.Error || Tokenizer.Error || ParseIntError;

    const Self = @This();

    pub fn new(src: []const u8) Self {
        return Self{ .tokenizer = Tokenizer.new(src), .token_prev = null, .token_curr = null };
    }

    pub fn parse(self: *Self) Error!Statement {
        try self.advance();
        if (try self.match(.Dot)) {
            return self.metaCommand();
        }
        return self.statement();
    }

    fn metaCommand(self: *Self) Error!Statement {
        if (!try self.match(.Ident)) {
            return Error.UnexpectedToken;
        }
        const command_name = self.token_prev orelse unreachable;

        if (std.mem.eql(u8, command_name.lex, "exit")) {
            return Statement{ .Command = CommandStatement.Exit };
        }
        return Error.UnrecognizedCommand;
    }

    fn statement(self: *Self) Error!Statement {
        if (try self.match(.Select)) {
            const row = try Row.new(0, "", "");
            return Statement{ .Query = .{ .typ = .Select, .row = row } };
        }
        if (try self.match(.Insert)) {
            const row_id = try self.integer();
            const row_key = try self.string();
            const row_val = try self.string();
            const row = try Row.new(row_id, row_key, row_val);
            return Statement{ .Query = .{ .typ = .Insert, .row = row } };
        }
        return Error.UnrecognizedCommand;
    }

    fn string(self: *Self) Error![]const u8 {
        if (!try self.match(.String)) {
            return Error.UnexpectedToken;
        }
        const s = self.token_prev.?.lex;
        return s[1 .. s.len - 1];
    }

    fn integer(self: *Self) Error!i32 {
        if (!try self.match(.Integer)) {
            return Error.UnexpectedToken;
        }
        return try std.fmt.parseInt(i32, self.token_prev.?.lex, 10);
    }

    fn advance(self: *Self) Error!void {
        self.token_prev = self.token_curr;
        self.token_curr = try self.tokenizer.next();
    }

    fn match(self: *Self, token_type: TokenType) Error!bool {
        const matched = self.check(token_type);
        if (matched) {
            try self.advance();
        }
        return matched;
    }

    fn check(self: *const Self, token_type: TokenType) bool {
        if (self.token_curr) |token| {
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

pub const CommandStatement = enum { Exit };

pub const QueryStatementType = enum {
    Insert,
    Select,
};

pub const QueryStatement = struct {
    typ: QueryStatementType,
    row: Row,
};
