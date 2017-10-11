from .reader import read_varint, read_binary_uint8, read_binary_int32
from .writer import write_varint, write_binary_uint8, write_binary_int32


class BlockInfo(object):
    is_overflows = False
    bucket_num = -1

    def write(self, buf):
        # Set of pairs (`FIELD_NUM`, value) in binary form. Then 0.
        write_varint(1, buf)
        write_binary_uint8(self.is_overflows, buf)

        write_varint(2, buf)
        write_binary_int32(self.bucket_num, buf)

        write_varint(0, buf)

    def read(self, buf):
        while True:
            field_num = read_varint(buf)
            if not field_num:
                break

            if field_num == 1:
                self.is_overflows = bool(read_binary_uint8(buf))

            elif field_num == 2:
                self.bucket_num = read_binary_int32(buf)


class Block(object):
    dict_row_types = (dict, )
    tuple_row_types = (list, tuple)
    supported_row_types = dict_row_types + tuple_row_types

    def __init__(self, columns_with_types=None, data=None, info=None,
                 types_check=False, received_from_server=False):
        self.columns_with_types = columns_with_types or []
        self.data = data or []  # stored in columnar orientation
        self.types_check = types_check

        if data and not received_from_server:
            # Guessing about whole data format by first row.
            rows = self.to_rows(data)
            self.data = self.to_columns(rows)

        self.info = info or BlockInfo()
        super(Block, self).__init__()

    def to_rows(self, data):
        first_row = data[0]

        if self.types_check:
            self.check_row_type(first_row)

        if isinstance(first_row, dict):
            return self.dicts_to_rows(data)
        else:
            return self.check_rows(data)

    def to_columns(self, rows):
        if not len(rows):
            return []
        elif not len(rows[0]):
            return []
        else:
            return list(zip(*rows))

    def dicts_to_rows(self, data):
        column_names = [x[0] for x in self.columns_with_types]

        check_row_type = False
        if self.types_check:
            check_row_type = self.check_dict_row_type

        rows = [None] * len(data)
        for i, row in enumerate(data):
            if check_row_type:
                check_row_type(row)

            rows[i] = [row[name] for name in column_names]

        return rows

    def check_rows(self, data):
        expected_row_len = len(self.columns_with_types)

        got = len(data[0])
        if expected_row_len != got:
            msg = 'Expected {} columns, got {}'.format(expected_row_len, got)
            raise ValueError(msg)

        check_row_type = False
        if self.types_check:
            check_row_type = self.check_tuple_row_type

        for row in data:
            if check_row_type:
                check_row_type(row)

            if len(row) != expected_row_len:
                raise ValueError('Different rows length')

        return data

    def get_columns(self):
        return self.data

    def get_rows(self):
        if not self.data:
            return self.data

        n_rows = self.rows
        n_columns = self.columns

        print('NROWS: {}; NCOLUMNS: {}'.format(n_rows, n_columns))
        print(self.columns_with_types)
        print(self.data)

        # Preallocate memory to avoid .append calls.
        rv = [None] * n_columns

        for i in range(n_rows):
            rv[i] = tuple([self.data[j][i] for j in range(n_columns)])

        return rv

    def check_row_type(self, row):
        if not isinstance(row, self.supported_row_types):
            raise TypeError(
                'Unsupported row type: {}. dict, list or tuple is expected.'
                .format(type(row))
            )

    def check_tuple_row_type(self, row):
        if not isinstance(row, self.tuple_row_types):
            raise TypeError(
                'Unsupported row type: {}. list or tuple is expected.'
                .format(type(row))
            )

    def check_dict_row_type(self, row):
        if not isinstance(row, self.dict_row_types):
            raise TypeError(
                'Unsupported row type: {}. dict is expected.'
                .format(type(row))
            )

    @property
    def columns(self):
        return len(self.data)

    @property
    def rows(self):
        return len(self.data[0]) if self.columns else 0
