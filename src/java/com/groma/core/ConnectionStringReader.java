package com.groma.core;

public class ConnectionStringReader {

    private String _connectionString;

    private int _offset, _length;

    private String _key, _name, _value;

    private static char[] QUOTES = new char[] {
            '\'','\'',
            '\"','\"',
            '{','}',
    };

    public ConnectionStringReader(String connectionString) {
        this._connectionString = connectionString;
        this._offset = 0;
        this._length = this._connectionString.length();
    }

    public boolean read() throws Exception {
        this._key = null;
        this._name = null;
        this._value = null;

        if (this._offset >= this._length) {
            return false;
        }

        while(this._offset < this._length) {
            this._name = this.readName();
            if (this._name != null) {
                break;
            }
        }

        if (this._name == null) {
            return false;
        }

        this._value = this.readValue();
        this._key = this.buildKey(this._name);
        return true;
    }

    public String getPropertyKey() {
        return this._key;
    }

    public String getPropertyName() {
        return this._name;
    }

    public String getPropertyValue() {
        return this._value;
    }

    private String readName() throws Exception {
        this.skipSpaces();

        int s = this._offset;
        int e = -1;
        for (; this._offset < this._length; this._offset++) {
            char c = this._connectionString.charAt(this._offset);
            if (c == '=') {
                e = this._offset - 1;
                break;
            } else if (c == ';') {
                this._offset++;
                return null;
            }
        }

        if (e == -1) {
            throw new Exception(String.format("Invalid connection string syntax at {0}", this._offset));
        }

        for (; e >= s; e--) {
            char c = this._connectionString.charAt(e);
            if (!Character.isSpaceChar(c)) {
                break;
            }
        }

        this._offset++;
        return this._connectionString.substring(s, e + 1);
    }

    private String readValue() {
        this.skipSpaces();
        int s = this._offset;
        int e = -1;

        char qe = '\0';
        for (; this._offset < this._length; this._offset++) {
            char c = this._connectionString.charAt(this._offset);
            if (qe == '\0') {
                if (c == ';') {
                    e = this._offset - 1;
                    break;
                } else if (c == '\'' || c == '\"' || c == '{') {
                    s = this._offset + 1;
                    qe = getEndQuote(c);
                    continue;
                }
            } else {
                if (qe == c) {
                    e = this._offset - 1;
                    break;
                }
            }
        }

        if (e == -1) {
            e = this._offset - 1;
        }

        if (qe == '\0') {
            for (; e >= s; e--) {
                char c = this._connectionString.charAt(e);
                if (!Character.isSpaceChar(c)) {
                    break;
                }
            }
        }

        this._offset++;
        return this._connectionString.substring(s, e + 1);
    }

    private String buildKey(String name) {
        StringBuilder key = null;
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if ((c >= 'A' && c <= 'Z') || Character.isSpaceChar(c)) {
                key = new StringBuilder(name.length());
            }
        }

        if (key == null) {
            return name;
        }

        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c >= 'A' && c <= 'Z') {
                key.append(Character.toLowerCase(c));
            } else if (!Character.isSpaceChar(c)) {
                key.append(c);
            }
        }

        return key.toString();
    }

    private char getEndQuote(char c) {
        if (c == '{') return '}';
        return c;
    }

    private void skipSpaces() {
        for (; this._offset < this._length; this._offset++) {
            char c = this._connectionString.charAt(this._offset);
            if (!Character.isSpaceChar(c)) {
                break;
            }
        }
    }
}
