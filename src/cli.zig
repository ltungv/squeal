const std = @import("std");

pub const MAX_LINE_BUFFER_SIZE = 1024;

/// A wrapper around a pair of StreamSource.Reader and StreamSource.Writer.
pub const Stream = struct {
    reader: std.io.StreamSource.Reader,
    writer: std.io.StreamSource.Writer,

    /// Error that occurs when writing to the stream.
    pub const WriteError = std.io.StreamSource.WriteError;

    /// Error that occurs when reading from the stream.
    pub const ReadError = error{ StreamTooLong, NoSpaceLeft } || std.io.StreamSource.ReadError;

    /// Create a new stream.
    pub fn new(istream: *std.io.StreamSource, ostream: *std.io.StreamSource) @This() {
        return .{ .reader = istream.reader(), .writer = ostream.writer() };
    }

    /// Print a simple string.
    pub fn print(this: *const @This(), comptime format: []const u8) WriteError!void {
        try this.writer.print(format, .{});
    }

    /// Print a formatted string.
    pub fn printf(this: *const @This(), comptime format: []const u8, args: anytype) WriteError!void {
        try this.writer.print(format, args);
    }

    /// Print an error in its default format.
    pub fn eprint(this: *const @This(), err: anyerror) WriteError!void {
        try this.writer.print("{!}\n", .{err});
    }

    /// Read a single line from the stream into an owned slice.
    pub fn readln(this: *const @This(), buf: []u8) ReadError!?[]u8 {
        return this.reader.readUntilDelimiterOrEof(buf, '\n');
    }
};
