class Table:
    def __init__(self, rows, cols):
        self.no_of_rows = rows
        self.no_of_cols = cols
        self.table = []

    def make(self):
        for _ in range(self.no_of_rows):
            col_vals = []
            for c in range(self.no_of_cols):
                col_vals.append('Undefined')
            self.table.append(col_vals)

    def __str__(self):
        rows = []
        string = ''
        coldata = []
        for row in self.table:
            for col in row:
                coldata.append(len(col))
        max_entry_len = max(coldata)
        for row in self.table:
            data = ''
            for col in row:
                data += '| ' + str(col) + (' ' * (max_entry_len - len(str(col)))) + ' |'
            rows.append(data)
        mno = len(max(rows))
        string += ('_' * mno) + '\n\n'
        for row in rows:
            string += row + '\n'
        string += ('_' * mno) + '\n'
        return string

    def put(self, row, col, val):
        row -= 1
        col -= 1
        val = str(val)
        if len(val) > 40:
            val = val[:40] + '...'
        self.table[row][col] = val


