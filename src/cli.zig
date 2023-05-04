const std = @import("std");

const MAX_LINE_BUFFER_SIZE = 1024;

pub const Stream = struct {
    reader: std.io.StreamSource.Reader,
    writer: std.io.StreamSource.Writer,

    pub const Error = error{
        EndOfStream,
        StreamTooLong,
    } || std.io.StreamSource.ReadError || std.io.StreamSource.WriteError || std.io.StreamSource.SeekError || std.mem.Allocator.Error;

    const Self = @This();

    pub fn new(istream: *std.io.StreamSource, ostream: *std.io.StreamSource) Self {
        return Self{ .reader = istream.reader(), .writer = ostream.writer() };
    }

    pub fn prompt(self: *const Self) Error!void {
        try self.writer.print("db > ", .{});
    }

    pub fn print(self: *const Self, comptime format: []const u8) Error!void {
        try self.writer.print(format, .{});
    }

    pub fn printf(self: *const Self, comptime format: []const u8, args: anytype) Error!void {
        try self.writer.print(format, args);
    }

    pub fn eprint(self: *const Self, err: anytype) Error!void {
        try self.writer.print("{!}\n", .{err});
    }

    pub fn readln(self: *const Self, allocator: std.mem.Allocator) Error![]u8 {
        const result = try self.reader.readUntilDelimiterAlloc(allocator, '\n', MAX_LINE_BUFFER_SIZE);
        return result;
    }
};
