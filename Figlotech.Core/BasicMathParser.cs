using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Figlotech.Core {
    public enum TokenType {
        Number,
        Plus,
        Minus,
        Multiply,
        Divide,
        Exponent,
        LeftParen,
        RightParen,
        EndOfExpression
    }

    public class Token {
        public TokenType Type { get; }
        public string Value { get; } // For numbers, holds the numeric string

        public Token(TokenType type, string value = null) {
            Type = type;
            Value = value;
        }
    }

    public class Tokenizer {
        private readonly string _expression;
        private int _position;
        private readonly int _length;

        public Tokenizer(string expression) {
            _expression = expression;
            _length = expression.Length;
            _position = 0;
        }

        public Token GetNextToken() {
            SkipWhitespace();

            if (_position >= _length)
                return new Token(TokenType.EndOfExpression);

            char current = _expression[_position];

            // Handle numbers (including decimal points and commas)
            if (char.IsDigit(current) || current == '.' || current == ',') {
                int start = _position;
                while (_position < _length && (char.IsDigit(_expression[_position]) || _expression[_position] == '.' || _expression[_position] == ',')) {
                    _position++;
                }
                string numberStr = _expression.Substring(start, _position - start);
                return new Token(TokenType.Number, numberStr);
            }

            // Handle operators and parentheses
            switch (current) {
                case '+':
                    _position++;
                    return new Token(TokenType.Plus);
                case '-':
                    _position++;
                    return new Token(TokenType.Minus);
                case '*':
                    _position++;
                    return new Token(TokenType.Multiply);
                case '/':
                    _position++;
                    return new Token(TokenType.Divide);
                case '^':
                    _position++;
                    return new Token(TokenType.Exponent);
                case '(':
                    _position++;
                    return new Token(TokenType.LeftParen);
                case ')':
                    _position++;
                    return new Token(TokenType.RightParen);
                default:
                    throw new ArgumentException($"Invalid character encountered: {current}");
            }
        }

        private void SkipWhitespace() {
            while (_position < _length && char.IsWhiteSpace(_expression[_position])) {
                _position++;
            }
        }
    }

    public class BasicMathParser {
        private readonly Tokenizer _tokenizer;
        private Token _currentToken;

        public BasicMathParser(string expression) {
            _tokenizer = new Tokenizer(expression);
            _currentToken = _tokenizer.GetNextToken();
        }

        public string ParseExpression() {
            try {
                decimal result = ParseAddSubtract();
                if (_currentToken.Type != TokenType.EndOfExpression)
                    throw new ArgumentException("Unexpected characters at end of expression.");

                return result.ToString(CultureInfo.CurrentCulture);
            } catch (DivideByZeroException) {
                return "NAN";
            } catch {
                // For any parsing errors, you might want to handle them differently
                return "NAN";
            }
        }

        private decimal ParseAddSubtract() {
            decimal result = ParseMultiplyDivide();

            while (_currentToken.Type == TokenType.Plus || _currentToken.Type == TokenType.Minus) {
                TokenType op = _currentToken.Type;
                Consume(_currentToken.Type);
                decimal right = ParseMultiplyDivide();

                if (op == TokenType.Plus)
                    result += right;
                else
                    result -= right;
            }

            return result;
        }

        private decimal ParseMultiplyDivide() {
            decimal result = ParseExponent();

            while (_currentToken.Type == TokenType.Multiply || _currentToken.Type == TokenType.Divide) {
                TokenType op = _currentToken.Type;
                Consume(_currentToken.Type);
                decimal right = ParseExponent();

                if (op == TokenType.Multiply)
                    result *= right;
                else {
                    if (right == 0)
                        throw new DivideByZeroException();
                    result /= right;
                }
            }

            return result;
        }

        private decimal ParseExponent() {
            decimal result = ParsePrimary();

            while (_currentToken.Type == TokenType.Exponent) {
                Consume(TokenType.Exponent);
                decimal exponent = ParseExponent(); // Right-associative
                result = (decimal)Math.Pow((double)result, (double)exponent);
            }

            return result;
        }

        private decimal ParsePrimary() {
            if (_currentToken.Type == TokenType.Number) {
                string numberStr = _currentToken.Value;
                Consume(TokenType.Number);
                if (!TryParseNumber(numberStr, out decimal number))
                    throw new ArgumentException($"Invalid number format: {numberStr}");
                return number;
            } else if (_currentToken.Type == TokenType.LeftParen) {
                Consume(TokenType.LeftParen);
                decimal expr = ParseAddSubtract();
                if (_currentToken.Type != TokenType.RightParen)
                    throw new ArgumentException("Missing closing parenthesis.");
                Consume(TokenType.RightParen);
                return expr;
            } else if (_currentToken.Type == TokenType.Minus) {
                // Handle unary minus
                Consume(TokenType.Minus);
                return -ParsePrimary();
            } else {
                throw new ArgumentException($"Unexpected token: {_currentToken.Type}");
            }
        }

        private void Consume(TokenType expectedType) {
            if (_currentToken.Type == expectedType) {
                _currentToken = _tokenizer.GetNextToken();
            } else {
                throw new ArgumentException($"Expected token {expectedType}, but found {_currentToken.Type}");
            }
        }

        private bool TryParseNumber(string numberStr, out decimal number) {
            // Try parsing with CurrentCulture
            if (decimal.TryParse(numberStr, NumberStyles.AllowDecimalPoint | NumberStyles.AllowThousands, CultureInfo.CurrentCulture, out number))
                return true;

            // Try parsing with InvariantCulture
            if (decimal.TryParse(numberStr, NumberStyles.AllowDecimalPoint | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out number))
                return true;

            return false;
        }
    }
}
