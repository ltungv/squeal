/// Error that occurs when trying to serialize a value to bytes.
pub const SerializeError = error{NoSpaceLeft};

/// Error that occurs when trying to deserialize a value from bytes.
pub const DeserializeError = error{ EndOfStream, InvalidValue };
