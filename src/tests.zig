const std = @import("std");
const squeal_vm = @import("vm.zig");

comptime {
    _ = @import("pager_test.zig");
    _ = @import("table_test.zig");
    _ = @import("vm_test.zig");
}

pub fn randomTemporaryFilePath(allocator: std.mem.Allocator) ![]u8 {
    const tmpdir = std.testing.tmpDir(.{});
    const tmpdir_path = try tmpdir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmpdir_path);

    var random_bytes: [32]u8 = undefined;
    var rng = std.rand.DefaultPrng.init(0);
    rng.random().bytes(&random_bytes);

    var random_subpath: [std.fs.base64_encoder.calcSize(32)]u8 = undefined;
    _ = std.fs.base64_encoder.encode(&random_subpath, &random_bytes);

    return std.fs.path.resolve(allocator, &[_][]const u8{ tmpdir_path, &random_subpath });
}

pub fn expectVmOutputGivenInput(allocator: std.mem.Allocator, path: []const u8, expected: []const u8, input: []const u8) !void {
    var output = try allocator.alloc(u8, expected.len);
    defer allocator.free(output);

    var istream = std.io.StreamSource{ .const_buffer = std.io.fixedBufferStream(input) };
    var ostream = std.io.StreamSource{ .buffer = std.io.fixedBufferStream(output) };
    const stream = squeal_vm.Stream.new(&istream, &ostream);

    var vm = try squeal_vm.Vm.init(allocator, &stream, path);
    defer vm.deinit();
    try vm.run();

    try std.testing.expectEqualStrings(expected, output);
}
