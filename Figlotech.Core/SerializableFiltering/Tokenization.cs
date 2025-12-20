using System;
using System.Collections.Generic;
using System.Text;

namespace MiniWhere {
    internal enum TokenKind {
        EOF,
        Identifier,
        String,
        Number,
        True,
        False,
        Null,

        LParen,
        RParen,
        Comma,
        Dot,

        Eq,
        Neq,
        Lt,
        Lte,
        Gt,
        Gte,

        And,
        Or,
        Not,
        In,
        Like,
        Is
    }

    internal sealed class Token {
        public Token(TokenKind kind, string text) {
            Kind = kind;
            Text = text;
        }

        public TokenKind Kind { get; }
        public string Text { get; }
    }

    internal sealed class Lexer {
        private readonly string _s;
        private int _i;

        public Lexer(string s) {
            _s = s;
        }

        public List<Token> Lex() {
            var tokens = new List<Token>();
            while (true) {
                SkipWs();
                if (_i >= _s.Length) {
                    tokens.Add(new Token(TokenKind.EOF, ""));
                    return tokens;
                }

                char c = _s[_i];

                // punctuation
                if (c == '(') { _i++; tokens.Add(new Token(TokenKind.LParen, "(")); continue; }
                if (c == ')') { _i++; tokens.Add(new Token(TokenKind.RParen, ")")); continue; }
                if (c == ',') { _i++; tokens.Add(new Token(TokenKind.Comma, ",")); continue; }
                if (c == '.') { _i++; tokens.Add(new Token(TokenKind.Dot, ".")); continue; }

                // operators
                if (c == '=') {
                    _i++; tokens.Add(new Token(TokenKind.Eq, "=")); continue;
                }
                if (c == '!' && Peek('=')) {
                    _i += 2; tokens.Add(new Token(TokenKind.Neq, "!=")); continue;
                }
                if (c == '<') {
                    if (Peek('=')) { _i += 2; tokens.Add(new Token(TokenKind.Lte, "<=")); } else { _i++; tokens.Add(new Token(TokenKind.Lt, "<")); }
                    continue;
                }
                if (c == '>') {
                    if (Peek('=')) { _i += 2; tokens.Add(new Token(TokenKind.Gte, ">=")); } else { _i++; tokens.Add(new Token(TokenKind.Gt, ">")); }
                    continue;
                }

                // string
                if (c == '"' || c == '\'') {
                    tokens.Add(new Token(TokenKind.String, ReadString()));
                    continue;
                }

                // number
                if (char.IsDigit(c) || (c == '-' && _i + 1 < _s.Length && char.IsDigit(_s[_i + 1]))) {
                    tokens.Add(new Token(TokenKind.Number, ReadNumber()));
                    continue;
                }

                // identifier / keywords
                if (char.IsLetter(c) || c == '_') {
                    string ident = ReadIdent();
                    switch (ident.ToUpperInvariant()) {
                        case "AND": tokens.Add(new Token(TokenKind.And, ident)); break;
                        case "OR": tokens.Add(new Token(TokenKind.Or, ident)); break;
                        case "NOT": tokens.Add(new Token(TokenKind.Not, ident)); break;
                        case "IN": tokens.Add(new Token(TokenKind.In, ident)); break;
                        case "LIKE": tokens.Add(new Token(TokenKind.Like, ident)); break;
                        case "IS": tokens.Add(new Token(TokenKind.Is, ident)); break;
                        case "TRUE": tokens.Add(new Token(TokenKind.True, ident)); break;
                        case "FALSE": tokens.Add(new Token(TokenKind.False, ident)); break;
                        case "NULL": tokens.Add(new Token(TokenKind.Null, ident)); break;
                        default: tokens.Add(new Token(TokenKind.Identifier, ident)); break;
                    }
                    continue;
                }

                throw new Exception($"Unexpected character '{c}' at position {_i}");
            }
        }

        private bool Peek(char expected) {
            return _i + 1 < _s.Length && _s[_i + 1] == expected;
        }

        private void SkipWs() {
            while (_i < _s.Length && char.IsWhiteSpace(_s[_i])) _i++;
        }

        private string ReadIdent() {
            int start = _i;
            _i++;
            while (_i < _s.Length && (char.IsLetterOrDigit(_s[_i]) || _s[_i] == '_')) _i++;
            return _s[start.._i];
        }

        private string ReadNumber() {
            int start = _i;
            _i++; // first digit or '-'
            while (_i < _s.Length && (char.IsDigit(_s[_i]) || _s[_i] == '.')) _i++;
            return _s[start.._i];
        }

        private string ReadString() {
            char quote = _s[_i];
            _i++;
            var sb = new StringBuilder();
            while (_i < _s.Length) {
                char c = _s[_i++];
                if (c == quote) break;
                if (c == '\\' && _i < _s.Length) {
                    char esc = _s[_i++];
                    sb.Append(esc switch {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        '\\' => '\\',
                        '"' => '"',
                        '\'' => '\'',
                        _ => esc
                    });
                } else sb.Append(c);
            }
            return sb.ToString();
        }
    }
}
