"""Exception definitions"""


class TapdataError(Exception):
    """Tapdata API error base class"""
    
    def __init__(self, resp: dict):
        self.resp = resp
        self.code = resp.get("code", "unknown")
        self.message = resp.get("message", str(resp))
        super().__init__(self.message)


class TapdataAuthError(TapdataError):
    """Authentication error"""
    pass


class TapdataConnectionError(TapdataError):
    """Connection error"""
    pass


class TapdataValidationError(TapdataError):
    """Validation error"""
    pass


class TapdataTimeoutError(TapdataError):
    """Timeout error"""
    pass
