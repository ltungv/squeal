const std = @import("std");
const cli = @import("cli.zig");

const Parser = @import("parse.zig").Parser;
const Statement = @import("parse.zig").Statement;
const Table = @import("table.zig").Table;

pub const Vm = struct {
    allocator: std.mem.Allocator,
    stream: *const cli.Stream,
    table: Table,

    const Error = cli.Stream.Error || Table.Error;

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, stream: *const cli.Stream, path: []const u8) Error!Self {
        return .{
            .allocator = allocator,
            .stream = stream,
            .table = try Table.init(allocator, path),
        };
    }

    pub fn deinit(self: *Self) void {
        self.table.deinit();
    }

    pub fn run(self: *Self) Error!void {
        var finished = false;
        while (!finished) {
            try self.stream.prompt();

            const line = self.stream.readln(self.allocator) catch |err| {
                try self.stream.eprint(err);
                continue;
            };
            defer self.allocator.free(line);

            var parser = Parser.new(line);
            const statement = parser.parse() catch |err| {
                try self.stream.eprint(err);
                continue;
            };

            finished = self.exec(&statement) catch |err| {
                try self.stream.eprint(err);
                continue;
            };
        }
    }

    fn exec(self: *Self, statement: *const Statement) Error!bool {
        switch (statement.*) {
            .Command => |command| switch (command) {
                .Exit => return true,
            },
            .Query => |query| switch (query.typ) {
                .Select => {
                    const rows = try self.table.select(self.allocator);
                    defer self.allocator.free(rows);

                    for (rows) |row| {
                        const key = row.key_buf[0..row.key_len];
                        const val = row.val_buf[0..row.val_len];
                        try self.stream.printf("({d}, {s}, {s})\n", .{ row.id, key, val });
                    }
                },
                .Insert => {
                    try self.table.insert(&query.row);
                },
            },
        }
        try self.stream.print("Executed.\n");
        return false;
    }
};
