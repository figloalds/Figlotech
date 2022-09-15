﻿using System;
using Figlotech.Core.I18n;

namespace Figlotech.Core {
    public sealed class BDadosEnglishStringsProvider : IBDadosStringsProvider {
        public string AUTH_USER_MAX_ATTEMPTS_EXCEEDED => "User login attempt exceeded, please wait {0} minute(s).";
        public string AUTH_USER_NOT_FOUND => "User is not registered.";
        public string AUTH_PASSWORD_INCORRECT => "Password provided is incorrect";
        public string AUTH_USER_BLOCKED => "User is blocked.";
        public string AUTH_PASSWORDS_MUST_MATCH => "Password and confirmation must match";
        public string AUTH_USER_ALREADY_EXISTS => "User already exists";
        public string BDIOC_CANNOT_RESOLVE_TYPE => "Could not resolve implementation for type '{0}'.";
        public string SCOPY_ACCESSORS_CANNOT_BE_SAME => "SmartCopy cannot copy from a repository back to itself";

        public string ERROR_IN_STRUCTURE_CHECK => "Error occured during structure check";

        public string RDBMS_CANNOT_CONNECT => "Unable to connect to the RDBMS: {0}";
    }
}