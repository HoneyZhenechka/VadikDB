import sys
import os

sys.path.insert(0, 'engine/')
import engine.bin_file as bin_py


def test_binfile():
    test_file = bin_py.BinFile("test.bin")
    test_file.open("w+")
    test_file.write_integer(22, 0, 1)
    test_file.write_bool(False, 1)
    test_file.write_str("vadik", 2, 32)
    test_file.write_float(2.006, 35)
    test_file.write_fixed_integer(877776, 44)
    result_int = test_file.read_integer(0, 1)
    result_bool = test_file.read_bool(1)
    result_str = test_file.read_str(2, 32)
    result_float = test_file.read_float(35)
    result_fixed_int = test_file.read_fixed_integer(44)
    test_file.close()
    os.remove("test.bin")
    assert result_int == 22
    assert not result_bool
    assert result_str == "vadik"
    assert result_float == 2.006
    assert result_fixed_int == 877776
