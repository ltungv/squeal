const std = @import("std");

/// The tokenizer for our Squeal.
pub const Tokenizer = struct {
    src: []const u8,
    lexeme_head: usize,
    lexeme_tail: usize,

    pub const Error = error{UnexpectedByte};

    /// Initialize a new Squeal tokenizer.
    pub fn new(src: []const u8) @This() {
        return .{
            .src = src,
            .lexeme_head = 0,
            .lexeme_tail = 0,
        };
    }

    /// Returns the next token in the source code.
    pub fn next(this: *@This()) Error!?Token {
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
pub const Token = struct {
    typ: TokenType,
    lex: []const u8,
};

/// All types of tokens in our Squeal.
pub const TokenType = enum {
    Dot,
    Count,
    Select,
    Insert,
    Ident,
    Integer,
    String,
    Null,
};
