/*
 * ParseRapidJsonEvents.h
 *
 *  Created on: Oct 6, 2014
 *      Author: charlie
 */

#pragma once

#include <mongo/client/dbclient.h>
#include <string>
#include <stdlib.h>
#include "rapidjson/reader.h"
#include "rapidjson/error/en.h"


namespace loader {

    /**
     * Set the default event for rapidjson::BaseReaderHandler to false
     * Help future proof
     */
    class BaseReaderHandler {
    public:
        bool Default() { return false; }
    };

    /**
     * Handles parsing of a stream into a BSON Object.
     * Setup to hook into rapidjson SAX style parsing
     * mongo's custom data types should show up as strings in subobjects
     */
    class ParseRapidJsonEvents : public rapidjson::BaseReaderHandler<rapidjson::UTF8<>,
        loader::BaseReaderHandler> {
    public:
        /**
         * @Param size Size of the builder buffer
         * _state is initially set to Value so that StartObject will call properly
         */
        ParseRapidJsonEvents(size_t size = 512) :
            _size{size} {
            reset();
        }

        /**
         * Clears the parser for reuse
         */
        void reset() {
            while(_stack.size()) _stack.pop();
            _state = Value;
            _bob = nullptr;
            _count = 0;
            _unwind = 0;
        }

        void bufferSizeSet(size_t size) { _size = size; }

        /**
         * Fail unhandled events
         */
        bool Default() { return false; }

        /**
         * Use this to get the resultant object
         */
        mongo::BSONObj obj() { return _bob->obj(); }

        bool Key(const Ch* str, rapidjson::SizeType len, bool copy);

        bool Null() {
            switch (_state) {
            case Value:
                _bob->appendNull();
                _state = Field;
                return true;
            default :
                return false;
            }
        }

        bool Bool(bool value) {
            switch (_state) {
            case Value:
                _bob->append(_field, value);
                _state = Field;
                return true;
            default :
                return false;
            }
        }

        bool Int(int value) {
            switch (_state) {
            case Value:
                _bob->append(_field, value);
                _state = Field;
                return true;
            case TimeStamp:
                return timeStampSet(value);
            case NumberLong:
                _bob->append(_field, (long long)value);
                _state = Unwind;
                return true;
            case MinKey :
                _bob->appendMinKey(_field);
                _state = Unwind;
                return true;
            case MaxKey :
                _bob->appendMaxKey(_field);
                _state = Unwind;
                return false;
            default :
                return false;
            }
        }

        bool Uint(unsigned value) {
            switch (_state) {
            case Value:
                _bob->append(_field, value);
                _state = Field;
            return true;
            case TimeStamp:
                return timeStampSet(value);
            case NumberLong:
                _bob->append(_field, (long long)value);
                _state = Unwind;
                return true;
            default :
                return false;
            }
        }

        bool Int64(int64_t value) {
            switch (_state) {
            case Value:
                _bob->append(_field, (long long)value);
                _state = Field;
            return true;
            case TimeStamp:
                return timeStampSet(value);
            case NumberLong:
                _bob->append(_field, (long long)value);
                _state = Unwind;
                return true;
            default :
                return false;
            }
        }

        bool Uint64(uint64_t value) {
            switch (_state) {
            case Value:
                _bob->append(_field, (long long)value);
                _state = Field;
                return true;
            case TimeStamp:
                return timeStampSet(value);
            case NumberLong:
                _bob->append(_field, (long long)value);
                _state = Unwind;
                return true;
            default :
                return false;
            }
        }

        bool Double(double value) {
            switch (_state) {
            case Value:
                _bob->append(_field, value);
                _state = Field;
                return true;
            default :
                return false;
            }
        }

        /**
         * String can also represent "other" types.
         */
        bool String(const Ch* str, rapidjson::SizeType size, bool copy);

        bool convertNumberLong(long long *value, const char* const str, size_t size) {
            char* endptr;
            *value = strtoll(str, &endptr, 10);
            //Size doesn't include \0
            if (str + size + 1 != endptr) return false;
            return true;
        }

        bool allDigits(const char* const str, size_t size) {
            for(size_t pos = 0; pos < size; ++pos)
                if(!isdigit(str[pos])) return false;
            return true;
        }

        bool StartObject() {
            switch (_state) {
            case Value:
                _state = EmbeddedStart;
                return true;
            case TimeStampStartSubObj :
                //We hit this for the value of $timestamp
                _state = TimeStamp;
                ++_unwind;
                return true;
            default :
                return false;
            }
        }

        bool EndObject(rapidjson::SizeType) {
            switch (_state) {
            /*
             * EndEmbedded objects, i.e. not real bson subobjects
             */
            case Field:
                if (_unwind == 0)
                    return subObjEnd();
                return false;
            case Unwind:
                //Ensure that there is unwinding to do
                if (!_unwind) return false;
                //If we have completed unwinding only a field is possible
                if (--_unwind == 0)
                    _state = Field;
                return true;
            default :
                return false;
            }
        }

        bool StartArray() {
            switch (_state) {
            case Value:
                subArrayStart(_field);
                return true;
            default :
                return false;
            }
        }

        bool EndArray(rapidjson::SizeType) {
            switch (_state) {
            case Field:
                return subArrayEnd();
            default :
                return false;
            }
        }

        bool timeStampSet(long long value) {
            if (!_count) {
                if (_subField != "t") return false;
                _stackTimeStampT = value;
                ++_count;
                return true;
            }
            if (_count > 1 || _subField != "i") return false;
            _bob->appendTimestamp(_field, _stackTimeStampT, value);
            //prior driver _bob->appendTimestamp(_field, mongo::Timestamp_t(_stackTimeStampT*1000, value));

            ++_count;
            _state = Unwind;
            return true;
        }

    private:
        struct Frame {
            const bool array;
            mongo::BSONObjBuilder bob;

            /**
             * ctor forwards args to the BSONObjBuilder so that it can take buffers
             */
            template<typename... Arg1>
            Frame(bool array_, Arg1... args) : array(array_), bob(args...)
            { }
        };

        /**
         * The current field name to operate on
         */
        std::string _field;

        /**
         * Temp value for the stack
         */
        std::string _subField;

        /**
         * Used to keep track of embedded types
         */
        size_t _count;

        /**
         * How deep to unwind without complaint and no values should be detected
         */
        size_t _unwind;

        /**
         * State tracks what the next valid values are for translation
         */
        enum State { EmbeddedStart, Unwind, Field, Value, OID, Binary, Date, TimeStampStartSubObj,
            TimeStamp, Regex, Ref, Undefined, NumberLong, MinKey, MaxKey, Finalized } _state;

        /**
         * Pointer to the current builder, this allows seamless transition to subobjects and arrays
         */
        mongo::BSONObjBuilder* _bob;

        /**
         * Size parameter to use with first _stack frame
         */
        size_t _size;

        /**
         * Stack that holds the reference to the current builder
         */
        std::stack<Frame> _stack;

        /**
         * t field of a timestamp type
         */
        long long _stackTimeStampT = 0;

        void setFrame() {
            _bob = &_stack.top().bob;
        }

        /*
         * The first object is signified by a pull pointer.
         * All other objects would use a parent supplied buffer
         */
        void subObjStart(const mongo::StringData &fieldName) {
            if (_bob)
                _stack.emplace(false, std::ref(_bob->subobjStart(fieldName)));
            else {
                _stack.emplace(false, _size);
            }
            setFrame();
        }

        bool subObjEnd() {
            if (_stack.size() == 0 || _stack.top().array != false) return false;
            //Is this the top frame, don't release it
            if(_stack.size() == 1) {
                _state = Finalized;
                setFrame();
                return true;
            }
            _bob->done();
            _stack.pop();
            setFrame();
            _state = Field;
            return true;
        }

        void subArrayStart(const mongo::StringData &fieldName) {
            _stack.emplace(true, std::ref(_bob->subarrayStart(fieldName)));
            setFrame();
        }

        bool subArrayEnd() {
            if (_stack.size() == 0 || _stack.top().array != true) return false;
            //If this is the top frame, don't release it
            if(_stack.size() == 1) {
                _state = Finalized;
                setFrame();
                return true;
            }
            _bob->done();
            _stack.pop();
            setFrame();
            _state = Field;
            return true;
        }
    };
} /* namespace loader */

