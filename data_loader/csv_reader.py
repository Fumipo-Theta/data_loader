import multiprocessing
from .csv_reader_poor_impl import CsvReader as CsvReaderPoor
from .csv_reader_rich_impl import CsvReader as CsvReaderRich

CsvReader = CsvReaderRich if multiprocessing.cpu_count() >= 10 else CsvReaderPoor
