const std = @import("std");
const cli = @import("cli.zig");
const squeal = @import("squeal.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var istream = std.io.StreamSource{ .file = std.io.getStdIn() };
    var ostream = std.io.StreamSource{ .file = std.io.getStdOut() };
    const stream = cli.Stream{ .reader = istream.reader(), .writer = ostream.writer() };

    var table = try squeal.Table.init(gpa.allocator(), "./db.squeal", @sizeOf(squeal.Row), 4096, 512);
    defer table.deinit() catch |err| {
        std.log.err("couldn't deinitialize the table: {!}", .{err});
    };

    var vm = try cli.Vm.init(gpa.allocator(), &stream, &table);
    try vm.run();
}

test {
    std.testing.refAllDecls(@This());
}
