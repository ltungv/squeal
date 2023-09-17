const std = @import("std");
const squeal_table = @import("table.zig");
const squeal_vm = @import("vm.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var istream = std.io.StreamSource{ .file = std.io.getStdIn() };
    var ostream = std.io.StreamSource{ .file = std.io.getStdOut() };
    const stream = squeal_vm.Stream.new(&istream, &ostream);

    var pager = try squeal_table.Pager.init(gpa.allocator(), "./db.squeal");
    defer pager.deinit();

    var table = try squeal_table.Table.init(&pager);
    defer table.deinit();

    var rng = std.rand.DefaultPrng.init(0);
    var count: u32 = 0;
    while (count < 15) : (count += 1) {
        var random_bytes: [16]u8 = undefined;

        rng.random().bytes(&random_bytes);
        var random_key: [std.fs.base64_encoder.calcSize(random_bytes.len)]u8 = undefined;
        _ = std.fs.base64_encoder.encode(&random_key, &random_bytes);

        rng.random().bytes(&random_bytes);
        var random_val: [std.fs.base64_encoder.calcSize(random_bytes.len)]u8 = undefined;
        _ = std.fs.base64_encoder.encode(&random_val, &random_bytes);

        const row = try squeal_table.Row.new(count, &random_key, &random_val);
        table.insert(&row) catch |err| {
            std.log.warn("insert error: {!}", .{err});
        };
    }

    var vm = try squeal_vm.Vm.init(gpa.allocator(), &stream, &table);
    try vm.run();
}
