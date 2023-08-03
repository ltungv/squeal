const std = @import("std");

const StreamSource = std.io.StreamSource;

pub const MAX_LINE_BUFFER_SIZE = 1024;

/// A wrapper around a pair of StreamSource.Reader and StreamSource.Writer.
pub const Stream = struct {
    reader: StreamSource.Reader,
    writer: StreamSource.Writer,

    /// Error that occurs when writing to the stream.
    pub const WriteError = StreamSource.WriteError;

    /// Error that occurs when reading from the stream.
    pub const ReadError = error{StreamTooLong} || StreamSource.ReadError;

    const Self = @This();

    /// Create a new Stream.
    pub fn new(istream: *StreamSource, ostream: *StreamSource) Self {
        return Self{ .reader = istream.reader(), .writer = ostream.writer() };
    }

    /// Print a simple string.
    pub fn print(self: *const Self, comptime format: []const u8) WriteError!void {
        try self.writer.print(format, .{});
    }

    /// Print a formatted string.
    pub fn printf(self: *const Self, comptime format: []const u8, args: anytype) WriteError!void {
        try self.writer.print(format, args);
    }

    /// Print an error in its default format.
    pub fn eprint(self: *const Self, err: anytype) WriteError!void {
        try self.writer.print("{!}\n", .{err});
    }

    /// Read a single line from the stream into an owned slice.
    pub fn readln(self: *const Self, buf: []u8) ReadError!?[]u8 {
        return self.reader.readUntilDelimiterOrEof(buf, '\n');
    }
};
