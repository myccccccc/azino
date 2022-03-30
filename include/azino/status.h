#ifndef AZINO_INCLUDE_STATUS_H
#define AZINO_INCLUDE_STATUS_H

#include <string>
#include <sstream>

namespace azino {
    class Status {
    public:
        ~Status() = default;
        Status static Ok(const std::string& s = "") {
            return Status(kOk, s);
        }
        Status static NotFound(const std::string& s = "") {
            return Status(kNotFound, s);
        }
        Status static NetworkErr(const std::string& s = "") {
            return Status(kNetworkErr, s);
        }
        Status static IllegalTxOp(const std::string& s = "") {
            return Status(kIllegalTxOp, s);
        }
        Status static NotSupportedErr(const std::string& s = "") {
            return Status(kNotSupportedErr, s);
        }
        bool IsOk() {
            return _error_code == kOk;
        }
        bool IsNotFound() {
            return _error_code == kNotFound;
        }
        bool IsNetWorkErr() {
            return _error_code == kNetworkErr;
        }
        bool IsIllegalTxOp() {
            return  _error_code == kIllegalTxOp;
        }
        bool IsNotSupportedErr() {
            return _error_code == kNotSupportedErr;
        }
        std::string ToString() {
            std::stringstream ss;
            std::string code_message;
            switch (_error_code) {
                case kOk:
                    code_message = "OK. ";
                    break;
                case kNotFound:
                    code_message = "NotFound. ";
                    break;
                case kNetworkErr:
                    code_message = "NetWorkError. ";
                    break;
                case kIllegalTxOp:
                    code_message = "IllegalTxOp. ";
                    break;
                case kNotSupportedErr:
                    code_message = "NotSupportedError. ";
                    break;
            }
            ss << code_message << _error_message;
            return ss.str();
        }
    private:
        enum Code {
            kOk = 0,
            kNotFound = 1,
            kNetworkErr = 2,
            kIllegalTxOp = 3,
            kNotSupportedErr = 4
        };
        Status(Code c, const std::string& s)
        : _error_code(c),
          _error_message(s) {}
        Code _error_code;
        std::string _error_message;
    };
}
#endif // AZINO_INCLUDE_STATUS_H