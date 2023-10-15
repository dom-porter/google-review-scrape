class EmptyBusinessError(Exception):
    """ Exception for accessing methods for an empty business instance """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class FactoryError(Exception):
    """ General exception for the factory method """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class BrowserError(Exception):
    """ General exception for browser/page manipulation errors"""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

