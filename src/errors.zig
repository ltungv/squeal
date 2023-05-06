pub const SerializeError = error{NoSpaceLeft};

pub const DeserializeError = error{ EndOfStream, InvalidValue };
