const std = @import("std");
const testing = std.testing;
const squeal_vm = @import("vm.zig");

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
    const output = try allocator.alloc(u8, expected.len);
    defer allocator.free(output);

    var istream = std.io.StreamSource{ .const_buffer = std.io.fixedBufferStream(input) };
    var ostream = std.io.StreamSource{ .buffer = std.io.fixedBufferStream(output) };
    const stream = squeal_vm.Stream{ .reader = istream.reader(), .writer = ostream.writer() };

    var pager = try squeal_vm.Table.Pager.init(allocator, path);
    defer pager.deinit();

    var table = try squeal_vm.Table.init(&pager);
    defer table.deinit() catch unreachable;

    var vm = try squeal_vm.Vm.init(allocator, &stream, &table);
    try vm.run();
    try std.testing.expectEqualStrings(expected, output);
}
