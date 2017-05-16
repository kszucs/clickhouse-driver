import importlib

from .. import errors
from ..protocol import CompressionMethodByte


def get_compressor_cls(alg):
    try:
        module = importlib.import_module('.' + alg, __name__)
        return module.Compressor

    except ImportError:
        raise errors.UnknownCompressionMethod()


def get_decompressor_cls(method_type):
    if method_type < 0x80:
        module = importlib.import_module('.quicklz', __name__)

    elif method_type == CompressionMethodByte.LZ4:
        module = importlib.import_module('.lz4', __name__)

    elif method_type == CompressionMethodByte.ZSTD:
        module = importlib.import_module('.zstd', __name__)

    else:
        raise errors.UnknownCompressionMethod()

    return module.Decompressor