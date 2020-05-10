
class Result:

    def __init__(self, is_exception: bool, str_for_print: str = "", user_index=0):
        self.is_exception = is_exception
        self.str_for_print = str_for_print
        self.user_index = user_index
