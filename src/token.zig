const std = @import("std");

/// The tokenizer for our Squeal.
pub const Tokenizer = struct {
    src: []const u8,
    lexeme_head: usize,
    lexeme_tail: usize,

    pub const Error = error{UnexpectedByte};

    const Self = @This();

    /// Initialize a new Squeal tokenizer.
    pub fn new(src: []const u8) Self {
        return Self{
            .src = src,
            .lexeme_head = 0,
            .lexeme_tail = 0,
        };
    }

    /// Returns the next token in the source code.
    pub fn next(self: *Self) Error!?Token {
        self.skipWhitespace();
        const byte = self.advance() orelse return null;
        if (byte == '.') {
            return self.makeToken(.Dot);
        }
        if (byte == '\'') {
            return try self.string();
        }
        if (byte == '-' or std.ascii.isDigit(byte)) {
            return try self.integer();
        }
        if (std.ascii.isAlphabetic(byte)) {
            return try self.identity();
        }
        return Error.UnexpectedByte;
    }

    fn isSingleQuote(byte: u8) bool {
        return byte == '\'';
    }

    fn string(self: *Self) Error!Token {
        while (!self.peekCheck(Self.isSingleQuote)) {
            _ = self.advance();
        }
        try self.consume('\'');
        return self.makeToken(.String);
    }

    fn identity(self: *Self) Error!Token {
        while (self.peekCheck(std.ascii.isAlphabetic)) {
            _ = self.advance();
        }
        var typ = TokenType.Ident;
        const lex = self.src[self.lexeme_head..self.lexeme_tail];
        if (std.mem.eql(u8, lex, "select")) {
            typ = .Select;
        }
        if (std.mem.eql(u8, lex, "insert")) {
            typ = .Insert;
        }
        if (std.mem.eql(u8, lex, "null")) {
            typ = .Null;
        }
        return self.makeToken(typ);
    }

    fn integer(self: *Self) Error!Token {
        while (self.peekCheck(std.ascii.isDigit)) {
            _ = self.advance();
        }
        return self.makeToken(.Integer);
    }

    fn makeToken(self: *const Self, typ: TokenType) Token {
        const lex = self.src[self.lexeme_head..self.lexeme_tail];
        return Token{ .typ = typ, .lex = lex };
    }

    fn skipWhitespace(self: *Self) void {
        while (self.peek()) |byte| {
            if (byte == ' ' or byte == '\r' or byte == '\t' or byte == '\n') {
                _ = self.advance();
            } else {
                break;
            }
        }
        self.lexeme_head = self.lexeme_tail;
    }

    fn advance(self: *Self) ?u8 {
        const byte = self.peek() orelse return null;
        self.lexeme_tail += 1;
        return byte;
    }

    fn consume(self: *Self, expected: u8) Error!void {
        const byte = self.advance() orelse return Error.UnexpectedByte;
        if (byte != expected) {
            return Error.UnexpectedByte;
        }
    }

    fn peek(self: *const Self) ?u8 {
        if (self.lexeme_tail >= self.src.len) {
            return null;
        }
        return self.src[self.lexeme_tail];
    }

    fn peekNext(self: *const Self) ?u8 {
        if (self.lexeme_tail + 1 >= self.src.len) {
            return null;
        }
        return self.src[self.lexeme_tail + 1];
    }

    fn peekCheck(self: *const Self, comptime predicate_fn: fn (u8) bool) bool {
        const byte = self.peek() orelse return false;
        return predicate_fn(byte);
    }
};

pub const Token = struct {
    typ: TokenType,
    lex: []const u8,
};

pub const TokenType = enum {
    Dot,
    Select,
    Insert,
    Ident,
    Integer,
    String,
    Null,
};
