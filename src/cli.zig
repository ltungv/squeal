const std = @import("std");

const StreamSource = std.io.StreamSource;

pub const MAX_LINE_BUFFER_SIZE = 1024;

pub const Stream = struct {
    reader: StreamSource.Reader,
    writer: StreamSource.Writer,

    pub const WriteError = StreamSource.WriteError;
    pub const ReadError = error{StreamTooLong} || StreamSource.ReadError;

    const Self = @This();

    pub fn new(istream: *StreamSource, ostream: *StreamSource) Self {
        return Self{ .reader = istream.reader(), .writer = ostream.writer() };
    }

    pub fn prompt(self: *const Self) WriteError!void {
        try self.writer.print("db > ", .{});
    }

    pub fn print(self: *const Self, comptime format: []const u8) WriteError!void {
        try self.writer.print(format, .{});
    }

    pub fn printf(self: *const Self, comptime format: []const u8, args: anytype) WriteError!void {
        try self.writer.print(format, args);
    }

    pub fn eprint(self: *const Self, err: anytype) WriteError!void {
        try self.writer.print("{!}\n", .{err});
    }

    pub fn readln(self: *const Self, buf: []u8) ReadError!?[]u8 {
        return self.reader.readUntilDelimiterOrEof(buf, '\n');
    }
};
