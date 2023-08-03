const std = @import("std");
const cli = @import("cli.zig");

const Vm = @import("vm.zig").Vm;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var istream = std.io.StreamSource{ .file = std.io.getStdIn() };
    var ostream = std.io.StreamSource{ .file = std.io.getStdOut() };
    const stream = cli.Stream.new(&istream, &ostream);

    var vm = try Vm.init(gpa.allocator(), &stream, "./db.squeal");
    defer vm.deinit();
    try vm.run();
}
