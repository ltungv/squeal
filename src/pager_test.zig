const std = @import("std");
const testing = std.testing;
const pager = @import("pager.zig");

const Node = pager.Node;
const NodeHeader = pager.NodeHeader;
const NodeType = pager.NodeType;
const NodeBody = pager.NodeBody;

const LeafNode = pager.LeafNode;
const LeafNodeCell = pager.LeafNodeCell;

const InternalNode = pager.InternalNode;
const InternalNodeCell = pager.InternalNodeCell;

const Row = @import("table.zig").Row;

test "serialize node leaf" {
    var leaf = LeafNode{
        .num_cells = LeafNode.MAX_CELLS,
        .cells = undefined,
    };
    for (leaf.cells) |*cell, cell_index| {
        const row = try Row.new(@intCast(u32, cell_index), "hello", "world");
        cell.* = LeafNodeCell{ .key = @intCast(u32, cell_index), .val = row };
    }
    var node = Node{
        .header = NodeHeader{ .is_root = 69, .parent = 420 },
        .body = NodeBody{ .Leaf = leaf },
    };

    var buf: [Node.SERIALIZED_LEAF_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try node.serialize(&ostream);

    var istream = std.io.fixedBufferStream(&buf);
    var reader = istream.reader();

    try testing.expectEqual(node.header.is_root, try reader.readInt(u8, .Little));
    try testing.expectEqual(node.header.parent, try reader.readInt(u32, .Little));
    try testing.expectEqual(NodeType.Leaf, try reader.readEnum(NodeType, .Little));
    try testing.expectEqual(node.body.Leaf.num_cells, try reader.readInt(u32, .Little));

    var cell_num: usize = 0;
    while (cell_num < node.body.Leaf.num_cells) : (cell_num += 1) {
        const cell = node.body.Leaf.cells[cell_num];
        try testing.expectEqual(cell.key, try reader.readInt(u32, .Little));
        try testing.expectEqual(cell.val.id, try reader.readInt(u32, .Little));
        try testing.expectEqual(cell.val.key_len, try reader.readInt(u8, .Little));
        try testing.expectEqual(cell.val.val_len, try reader.readInt(u8, .Little));
        try testing.expectEqual(cell.val.key_buf, try reader.readBytesNoEof(Row.MAX_KEY_LEN));
        try testing.expectEqual(cell.val.val_buf, try reader.readBytesNoEof(Row.MAX_VAL_LEN));
    }
}

test "deserialize node leaf" {
    var leaf = LeafNode{
        .num_cells = LeafNode.MAX_CELLS,
        .cells = undefined,
    };
    for (leaf.cells) |*cell, cell_index| {
        const row = try Row.new(@intCast(u32, cell_index), "hello", "world");
        cell.* = LeafNodeCell{ .key = @intCast(u32, cell_index), .val = row };
    }
    var node = Node{
        .header = NodeHeader{ .is_root = 69, .parent = 420 },
        .body = NodeBody{ .Leaf = leaf },
    };

    var buf: [Node.SERIALIZED_LEAF_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try node.serialize(&ostream);

    var istream = std.io.fixedBufferStream(@as([]const u8, &buf));
    var new_node: Node = undefined;
    try new_node.deserialize(&istream);

    try testing.expectEqual(node, new_node);
}

test "serialize node internal" {
    var internal = InternalNode{
        .num_keys = InternalNode.MAX_KEYS,
        .right_child = 0,
        .cells = undefined,
    };
    for (internal.cells) |*cell, cell_index| {
        cell.child = @intCast(u32, cell_index + 1);
        cell.key = @intCast(u32, cell_index);
    }
    var node = Node{
        .header = NodeHeader{ .is_root = 69, .parent = 420 },
        .body = NodeBody{ .Internal = internal },
    };

    var buf: [Node.SERIALIZED_INTERNAL_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try node.serialize(&ostream);

    var istream = std.io.fixedBufferStream(&buf);
    var reader = istream.reader();

    try testing.expectEqual(node.header.is_root, try reader.readInt(u8, .Little));
    try testing.expectEqual(node.header.parent, try reader.readInt(u32, .Little));
    try testing.expectEqual(NodeType.Internal, try reader.readEnum(NodeType, .Little));
    try testing.expectEqual(node.body.Internal.num_keys, try reader.readInt(u32, .Little));
    try testing.expectEqual(node.body.Internal.right_child, try reader.readInt(u32, .Little));

    var cell_num: usize = 0;
    while (cell_num < node.body.Internal.num_keys) : (cell_num += 1) {
        const cell = node.body.Internal.cells[cell_num];
        try testing.expectEqual(cell.child, try reader.readInt(u32, .Little));
        try testing.expectEqual(cell.key, try reader.readInt(u32, .Little));
    }
}

test "deserialize node internal" {
    var internal = InternalNode{
        .num_keys = InternalNode.MAX_KEYS,
        .right_child = 0,
        .cells = undefined,
    };
    for (internal.cells) |*cell, cell_index| {
        cell.child = @intCast(u32, cell_index + 1);
        cell.key = @intCast(u32, cell_index);
    }
    var node = Node{
        .header = NodeHeader{ .is_root = 69, .parent = 420 },
        .body = NodeBody{ .Internal = internal },
    };

    var buf: [Node.SERIALIZED_INTERNAL_SIZE]u8 = undefined;
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

test "serialize node body leaf" {
    var leaf = LeafNode{
        .num_cells = LeafNode.MAX_CELLS,
        .cells = undefined,
    };
    for (leaf.cells) |*cell, cell_index| {
        const row = try Row.new(@intCast(u32, cell_index), "hello", "world");
        cell.* = LeafNodeCell{ .key = @intCast(u32, cell_index), .val = row };
    }
    var body = NodeBody{ .Leaf = leaf };

    var buf: [NodeBody.SERIALIZED_LEAF_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try body.serialize(&ostream);

    var istream = std.io.fixedBufferStream(&buf);
    var reader = istream.reader();

    try testing.expectEqual(NodeType.Leaf, try reader.readEnum(NodeType, .Little));
    try testing.expectEqual(leaf.num_cells, try reader.readInt(u32, .Little));

    var cell_num: usize = 0;
    while (cell_num < leaf.num_cells) : (cell_num += 1) {
        const cell = leaf.cells[cell_num];
        try testing.expectEqual(cell.key, try reader.readInt(u32, .Little));
        try testing.expectEqual(cell.val.id, try reader.readInt(u32, .Little));
        try testing.expectEqual(cell.val.key_len, try reader.readInt(u8, .Little));
        try testing.expectEqual(cell.val.val_len, try reader.readInt(u8, .Little));
        try testing.expectEqual(cell.val.key_buf, try reader.readBytesNoEof(Row.MAX_KEY_LEN));
        try testing.expectEqual(cell.val.val_buf, try reader.readBytesNoEof(Row.MAX_VAL_LEN));
    }
}

test "deserialize node body leaf" {
    var leaf = LeafNode{
        .num_cells = LeafNode.MAX_CELLS,
        .cells = undefined,
    };
    for (leaf.cells) |*cell, cell_index| {
        const row = try Row.new(@intCast(u32, cell_index), "hello", "world");
        cell.* = LeafNodeCell{ .key = @intCast(u32, cell_index), .val = row };
    }
    var body = NodeBody{ .Leaf = leaf };

    var buf: [NodeBody.SERIALIZED_LEAF_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try body.serialize(&ostream);

    var istream = std.io.fixedBufferStream(@as([]const u8, &buf));
    var new_body: NodeBody = undefined;
    try new_body.deserialize(&istream);

    try testing.expectEqual(body, new_body);
}

test "serialize node body internal" {
    var internal = InternalNode{
        .num_keys = InternalNode.MAX_KEYS,
        .right_child = 0,
        .cells = undefined,
    };
    for (internal.cells) |*cell, cell_index| {
        cell.child = @intCast(u32, cell_index + 1);
        cell.key = @intCast(u32, cell_index);
    }
    var body = NodeBody{ .Internal = internal };

    var buf: [NodeBody.SERIALIZED_INTERNAL_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try body.serialize(&ostream);

    var istream = std.io.fixedBufferStream(&buf);
    var reader = istream.reader();

    try testing.expectEqual(NodeType.Internal, try reader.readEnum(NodeType, .Little));
    try testing.expectEqual(internal.num_keys, try reader.readInt(u32, .Little));
    try testing.expectEqual(internal.right_child, try reader.readInt(u32, .Little));

    var cell_num: usize = 0;
    while (cell_num < internal.num_keys) : (cell_num += 1) {
        const cell = internal.cells[cell_num];
        try testing.expectEqual(cell.child, try reader.readInt(u32, .Little));
        try testing.expectEqual(cell.key, try reader.readInt(u32, .Little));
    }
}

test "deserialize node body internal" {
    var internal = InternalNode{
        .num_keys = InternalNode.MAX_KEYS,
        .right_child = 0,
        .cells = undefined,
    };
    for (internal.cells) |*cell, cell_index| {
        cell.child = @intCast(u32, cell_index + 1);
        cell.key = @intCast(u32, cell_index);
    }
    var body = NodeBody{ .Internal = internal };

    var buf: [NodeBody.SERIALIZED_INTERNAL_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try body.serialize(&ostream);

    var istream = std.io.fixedBufferStream(@as([]const u8, &buf));
    var new_body: NodeBody = undefined;
    try new_body.deserialize(&istream);

    try testing.expectEqual(body, new_body);
}

test "serialize leaf node" {
    var leaf = LeafNode{
        .num_cells = LeafNode.MAX_CELLS,
        .cells = undefined,
    };
    for (leaf.cells) |*cell, cell_index| {
        const row = try Row.new(@intCast(u32, cell_index), "hello", "world");
        cell.* = LeafNodeCell{ .key = @intCast(u32, cell_index), .val = row };
    }

    var buf: [LeafNode.SERIALIZED_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try leaf.serialize(&ostream);

    var istream = std.io.fixedBufferStream(&buf);
    var reader = istream.reader();

    try testing.expectEqual(leaf.num_cells, try reader.readInt(u32, .Little));
    var cell_num: usize = 0;
    while (cell_num < leaf.num_cells) : (cell_num += 1) {
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
        .num_cells = LeafNode.MAX_CELLS,
        .cells = undefined,
    };
    for (leaf.cells) |*cell, cell_index| {
        const row = try Row.new(@intCast(u32, cell_index), "hello", "world");
        cell.* = LeafNodeCell{ .key = @intCast(u32, cell_index), .val = row };
    }

    var buf: [LeafNode.SERIALIZED_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try leaf.serialize(&ostream);

    var istream = std.io.fixedBufferStream(@as([]const u8, &buf));
    var new_node: LeafNode = undefined;
    try new_node.deserialize(&istream);

    try testing.expectEqual(leaf, new_node);
}

test "serialize leaf node cell" {
    const row = try Row.new(0, "hello", "world");
    const cell = LeafNodeCell{ .key = 0, .val = row };

    var buf: [LeafNodeCell.SERIALIZED_SIZE]u8 = undefined;
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

test "deserialize leaf node cell" {
    const row = try Row.new(0, "hello", "world");
    const cell = LeafNodeCell{ .key = 0, .val = row };

    var buf: [LeafNodeCell.SERIALIZED_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try cell.serialize(&ostream);

    var istream = std.io.fixedBufferStream(@as([]const u8, &buf));
    var new_cell: LeafNodeCell = undefined;
    try new_cell.deserialize(&istream);

    try testing.expectEqual(cell, new_cell);
}

test "serialize internal node" {
    var internal = InternalNode{
        .num_keys = InternalNode.MAX_KEYS,
        .right_child = 0,
        .cells = undefined,
    };
    for (internal.cells) |*cell, cell_index| {
        cell.child = @intCast(u32, cell_index + 1);
        cell.key = @intCast(u32, cell_index);
    }

    var buf: [InternalNode.SERIALIZED_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try internal.serialize(&ostream);

    var istream = std.io.fixedBufferStream(&buf);
    var reader = istream.reader();

    try testing.expectEqual(internal.num_keys, try reader.readInt(u32, .Little));
    try testing.expectEqual(internal.right_child, try reader.readInt(u32, .Little));
    var cell_num: usize = 0;
    while (cell_num < internal.num_keys) : (cell_num += 1) {
        const cell = internal.cells[cell_num];
        try testing.expectEqual(cell.child, try reader.readInt(u32, .Little));
        try testing.expectEqual(cell.key, try reader.readInt(u32, .Little));
    }
}

test "deserialize internal node" {
    var internal = InternalNode{
        .num_keys = InternalNode.MAX_KEYS,
        .right_child = 0,
        .cells = undefined,
    };
    for (internal.cells) |*cell, cell_index| {
        cell.child = @intCast(u32, cell_index + 1);
        cell.key = @intCast(u32, cell_index);
    }

    var buf: [InternalNode.SERIALIZED_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try internal.serialize(&ostream);

    var istream = std.io.fixedBufferStream(@as([]const u8, &buf));
    var new_node: InternalNode = undefined;
    try new_node.deserialize(&istream);

    try testing.expectEqual(internal, new_node);
}

test "serialize internal node cell" {
    const cell = InternalNodeCell{ .key = 0xDEAD, .child = 0xBEEF };

    var buf: [InternalNodeCell.SERIALIZED_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try cell.serialize(&ostream);

    var istream = std.io.fixedBufferStream(&buf);
    var reader = istream.reader();

    try testing.expectEqual(cell.child, try reader.readInt(u32, .Little));
    try testing.expectEqual(cell.key, try reader.readInt(u32, .Little));
}

test "deserialize internal node cell" {
    const cell = InternalNodeCell{ .key = 0xDEAD, .child = 0xBEEF };

    var buf: [InternalNodeCell.SERIALIZED_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try cell.serialize(&ostream);

    var istream = std.io.fixedBufferStream(@as([]const u8, &buf));
    var new_cell: InternalNodeCell = undefined;
    try new_cell.deserialize(&istream);

    try testing.expectEqual(cell, new_cell);
}
