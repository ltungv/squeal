const std = @import("std");
const squeal_vm = @import("vm.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var istream = std.io.StreamSource{ .file = std.io.getStdIn() };
    var ostream = std.io.StreamSource{ .file = std.io.getStdOut() };
    const stream = squeal_vm.Stream.new(&istream, &ostream);

    var pager = try squeal_vm.Table.Pager.init(gpa.allocator(), "./db.squeal");
    defer pager.deinit();

    var table = try squeal_vm.Table.init(&pager);
    defer table.deinit() catch |err| {
        std.log.err("couldn't deinitialize the table: {!}", .{err});
    };

    var vm = try squeal_vm.Vm.init(gpa.allocator(), &stream, &table);
    try vm.run();
}
