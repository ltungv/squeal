const std = @import("std");
const squeal_vm = @import("vm.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var istream = std.io.StreamSource{ .file = std.io.getStdIn() };
    var ostream = std.io.StreamSource{ .file = std.io.getStdOut() };
    const stream = squeal_vm.Stream.new(&istream, &ostream);

    var vm = try squeal_vm.Vm.init(gpa.allocator(), &stream, "./db.squeal");
    defer vm.deinit();
    try vm.run();
}
