const std = @import("std");
const testing = std.testing;
const pager = @import("pager.zig");

const Node = pager.Node;
const NodeHeader = pager.NodeHeader;
const NodeType = pager.NodeType;
const NodeBody = pager.NodeBody;

const LeafNodeHeader = pager.LeafNodeHeader;
const LeafNode = pager.LeafNode;
const Cell = pager.Cell;

const Row = @import("table.zig").Row;

test "serialize node" {
    var leaf = LeafNode{
        .header = .{ .num_cells = LeafNode.MAX_CELLS },
        .cells = undefined,
    };
    for (leaf.cells) |*cell, cell_index| {
        const row = try Row.new(@intCast(u32, cell_index), "hello", "world");
        cell.* = Cell{ .key = @intCast(u32, cell_index), .val = row };
    }
    var node = Node{
        .header = NodeHeader{ .is_root = 69, .parent = 420 },
        .body = NodeBody{ .Leaf = leaf },
    };

    var buf: [NodeHeader.SERIALIZED_SIZE + LeafNodeHeader.SERIALIZED_SIZE + LeafNode.SPACE_FOR_CELLS]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try node.serialize(&ostream);

    var istream = std.io.fixedBufferStream(&buf);
    var reader = istream.reader();

    try testing.expectEqual(node.header.is_root, try reader.readInt(u8, .Little));
    try testing.expectEqual(node.header.parent, try reader.readInt(u32, .Little));
    try testing.expectEqual(NodeType.Leaf, try reader.readEnum(NodeType, .Little));
    try testing.expectEqual(node.body.Leaf.header.num_cells, try reader.readInt(u32, .Little));

    var cell_num: usize = 0;
    while (cell_num < node.body.Leaf.header.num_cells) : (cell_num += 1) {
        const cell = node.body.Leaf.cells[cell_num];
        try testing.expectEqual(cell.key, try reader.readInt(u32, .Little));
        try testing.expectEqual(cell.val.id, try reader.readInt(u32, .Little));
        try testing.expectEqual(cell.val.key_len, try reader.readInt(u8, .Little));
        try testing.expectEqual(cell.val.val_len, try reader.readInt(u8, .Little));
        try testing.expectEqual(cell.val.key_buf, try reader.readBytesNoEof(Row.MAX_KEY_LEN));
        try testing.expectEqual(cell.val.val_buf, try reader.readBytesNoEof(Row.MAX_VAL_LEN));
    }
}

test "deserialize node" {
    var leaf = LeafNode{
        .header = .{ .num_cells = LeafNode.MAX_CELLS },
        .cells = undefined,
    };
    for (leaf.cells) |*cell, cell_index| {
        const row = try Row.new(@intCast(u32, cell_index), "hello", "world");
        cell.* = Cell{ .key = @intCast(u32, cell_index), .val = row };
    }
    var node = Node{
        .header = NodeHeader{ .is_root = 69, .parent = 420 },
        .body = NodeBody{ .Leaf = leaf },
    };

    var buf: [NodeHeader.SERIALIZED_SIZE + LeafNodeHeader.SERIALIZED_SIZE + LeafNode.SPACE_FOR_CELLS]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try node.serialize(&ostream);

    var istream = std.io.fixedBufferStream(@as([]const u8, &buf));
    var new_node: Node = undefined;
    try new_node.deserialize(&istream);

    try testing.expectEqual(node, new_node);
}

test "serialize node header" {
    var header = NodeHeader{ .is_root = 69, .parent = 420 };

    var buf: [NodeHeader.SERIALIZED_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try header.serialize(&ostream);

    var istream = std.io.fixedBufferStream(&buf);
    var reader = istream.reader();

    try testing.expectEqual(header.is_root, try reader.readInt(u8, .Little));
    try testing.expectEqual(header.parent, try reader.readInt(u32, .Little));
}

test "deserialize node header" {
    var header = NodeHeader{ .is_root = 69, .parent = 420 };

    var buf: [NodeHeader.SERIALIZED_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try header.serialize(&ostream);

    var istream = std.io.fixedBufferStream(@as([]const u8, &buf));
    var new_header: NodeHeader = undefined;
    try new_header.deserialize(&istream);

    try testing.expectEqual(header, new_header);
}

test "serialize node body" {
    var leaf = LeafNode{
        .header = .{ .num_cells = LeafNode.MAX_CELLS },
        .cells = undefined,
    };
    for (leaf.cells) |*cell, cell_index| {
        const row = try Row.new(@intCast(u32, cell_index), "hello", "world");
        cell.* = Cell{ .key = @intCast(u32, cell_index), .val = row };
    }
    var body = NodeBody{ .Leaf = leaf };

    var buf: [LeafNodeHeader.SERIALIZED_SIZE + LeafNode.SPACE_FOR_CELLS]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try body.serialize(&ostream);

    var istream = std.io.fixedBufferStream(&buf);
    var reader = istream.reader();

    try testing.expectEqual(NodeType.Leaf, try reader.readEnum(NodeType, .Little));
    try testing.expectEqual(leaf.header.num_cells, try reader.readInt(u32, .Little));

    var cell_num: usize = 0;
    while (cell_num < leaf.header.num_cells) : (cell_num += 1) {
        const cell = leaf.cells[cell_num];
        try testing.expectEqual(cell.key, try reader.readInt(u32, .Little));
        try testing.expectEqual(cell.val.id, try reader.readInt(u32, .Little));
        try testing.expectEqual(cell.val.key_len, try reader.readInt(u8, .Little));
        try testing.expectEqual(cell.val.val_len, try reader.readInt(u8, .Little));
        try testing.expectEqual(cell.val.key_buf, try reader.readBytesNoEof(Row.MAX_KEY_LEN));
        try testing.expectEqual(cell.val.val_buf, try reader.readBytesNoEof(Row.MAX_VAL_LEN));
    }
}

test "deserialize node body" {
    var leaf = LeafNode{
        .header = .{ .num_cells = LeafNode.MAX_CELLS },
        .cells = undefined,
    };
    for (leaf.cells) |*cell, cell_index| {
        const row = try Row.new(@intCast(u32, cell_index), "hello", "world");
        cell.* = Cell{ .key = @intCast(u32, cell_index), .val = row };
    }
    var body = NodeBody{ .Leaf = leaf };

    var buf: [LeafNodeHeader.SERIALIZED_SIZE + LeafNode.SPACE_FOR_CELLS]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try body.serialize(&ostream);

    var istream = std.io.fixedBufferStream(@as([]const u8, &buf));
    var new_body: NodeBody = undefined;
    try new_body.deserialize(&istream);

    try testing.expectEqual(body, new_body);
}

test "serialize leaf node" {
    var leaf = LeafNode{
        .header = .{ .num_cells = LeafNode.MAX_CELLS },
        .cells = undefined,
    };
    for (leaf.cells) |*cell, cell_index| {
        const row = try Row.new(@intCast(u32, cell_index), "hello", "world");
        cell.* = Cell{ .key = @intCast(u32, cell_index), .val = row };
    }

    var buf: [LeafNodeHeader.SERIALIZED_SIZE + LeafNode.SPACE_FOR_CELLS]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try leaf.serialize(&ostream);

    var istream = std.io.fixedBufferStream(&buf);
    var reader = istream.reader();

    try testing.expectEqual(leaf.header.num_cells, try reader.readInt(u32, .Little));
    var cell_num: usize = 0;
    while (cell_num < leaf.header.num_cells) : (cell_num += 1) {
        const cell = leaf.cells[cell_num];
        try testing.expectEqual(cell.key, try reader.readInt(u32, .Little));
        try testing.expectEqual(cell.val.id, try reader.readInt(u32, .Little));
        try testing.expectEqual(cell.val.key_len, try reader.readInt(u8, .Little));
        try testing.expectEqual(cell.val.val_len, try reader.readInt(u8, .Little));
        try testing.expectEqual(cell.val.key_buf, try reader.readBytesNoEof(Row.MAX_KEY_LEN));
        try testing.expectEqual(cell.val.val_buf, try reader.readBytesNoEof(Row.MAX_VAL_LEN));
    }
}

test "deserialize leaf node" {
    var leaf = LeafNode{
        .header = .{ .num_cells = LeafNode.MAX_CELLS },
        .cells = undefined,
    };
    for (leaf.cells) |*cell, cell_index| {
        const row = try Row.new(@intCast(u32, cell_index), "hello", "world");
        cell.* = Cell{ .key = @intCast(u32, cell_index), .val = row };
    }

    var buf: [LeafNodeHeader.SERIALIZED_SIZE + LeafNode.SPACE_FOR_CELLS]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try leaf.serialize(&ostream);

    var istream = std.io.fixedBufferStream(@as([]const u8, &buf));
    var new_node: LeafNode = undefined;
    try new_node.deserialize(&istream);

    try testing.expectEqual(leaf, new_node);
}

test "serialize cell" {
    const row = try Row.new(0x0102BEEF, "hello", "world");
    const cell = Cell{ .key = 0x0102BEEF, .val = row };

    var buf: [Cell.SERIALIZED_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try cell.serialize(&ostream);

    var istream = std.io.fixedBufferStream(&buf);
    var reader = istream.reader();

    try testing.expectEqual(cell.key, try reader.readInt(u32, .Little));
    try testing.expectEqual(cell.val.id, try reader.readInt(u32, .Little));
    try testing.expectEqual(cell.val.key_len, try reader.readInt(u8, .Little));
    try testing.expectEqual(cell.val.val_len, try reader.readInt(u8, .Little));
    try testing.expectEqual(cell.val.key_buf, try reader.readBytesNoEof(Row.MAX_KEY_LEN));
    try testing.expectEqual(cell.val.val_buf, try reader.readBytesNoEof(Row.MAX_VAL_LEN));
}

test "deserialize cell" {
    const row = try Row.new(0x0102BEEF, "hello", "world");
    const cell = Cell{ .key = 0x0102BEEF, .val = row };

    var buf: [Cell.SERIALIZED_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try cell.serialize(&ostream);

    var istream = std.io.fixedBufferStream(@as([]const u8, &buf));
    var new_cell: Cell = undefined;
    try new_cell.deserialize(&istream);

    try testing.expectEqual(cell, new_cell);
}
