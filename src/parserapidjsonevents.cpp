/*
 * ParseRapidJsonEvents.tools
 *
 *  Created on: Oct 6, 2014
 *      Author: charlie
 */

#include "parserapidjsonevents.h"

namespace loader {

    //TODO: Add $<special> keys such that mongoexport is supported
    //size doesn't include the null character
    bool ParseRapidJsonEvents::Key(const Ch* str, rapidjson::SizeType size, bool copy) {
        switch (_state) {
        //{ "_id" : { "$oid" : "54320744335b5783110229ef" }, "b" : NaN, "c" : { "$undefined" : true }, "d" : Infinity, "e" : -Infinity }
        case EmbeddedStart:
            /**
             * Check to see if we are possibly in a bson embedded type
             * If it is, don't record the field name, it isn't needed
             * Ensure that we have a stack, and if so the owner of this obj isn't an array
             */
            if(str[0] == '$' && _stack.size() && _stack.top().array == false) {
                if (strcmp(str + 1, "maxkey") == 0) {
                    _bob->appendMaxKey(_field);
                    _state = MaxKey;
                    _unwind = 1;
                    return true;
                }
                if (strcmp(str + 1, "minkey") == 0) {
                    _state = MinKey;
                    _unwind = 1;
                    return true;
                }
                if (strcmp(str + 1, "oid") == 0) {
                    //"field" : { "$oid" : "5431efe9f7f864f612455fed" }
                    _state = OID;
                    _unwind = 1;
                    return true;
                }
                if (strcmp(str + 1, "binary") == 0) {
                    //TOOD: implement binary
                    _state = Binary;
                    _unwind = 1;
                    return true;
                }
                if (strcmp(str + 1, "date") == 0) {
                    //"field" : { "$date" : "2014-10-05T21:26:58.957-0400" }
                    _state = Date;
                    _unwind = 1;
                    return true;
                }
                if (strcmp(str + 1, "timestamp") == 0) {
                    //"field" : { "$timestamp" : { "t" : 1412558825, "i" : 1 } }
                    //We assume that t is ALWAYS first
                    _state = TimeStampStartSubObj;
                    _count = 0;
                    _unwind = 1;
                    return true;
                }
                if (strcmp(str + 1, "regex") == 0) {
                    //TODO: implement regex
                    _state = Regex;
                    _unwind = 1;
                    return true;
                }
                if (strcmp(str + 1, "ref") == 0) {
                    //TODO implement Ref
                    _state = Ref;
                    _unwind = 1;
                    return true;
                }
                if (strcmp(str + 1, "undefined") == 0) {
                    _state = Undefined;
                    _unwind = 1;
                    return true;
                }
                if (strcmp(str + 1, "numberLong") == 0) {
                    _state = NumberLong;
                    _unwind = 1;
                    return true;
                }
            }
            //If start isn't a special mongo meta-type, then it is a valid subobject, start it
            //_field here is the old field
            subObjStart(_field);
            //No break, if the start isn't special let it fall through
        case Field:
            _field.assign(str, size);
            _state = Value;
            return true;
        case TimeStampStartSubObj:
            //We hit this when starting the "t" field.
            _subField.assign(str, size);
            return true;
        default :
            return false;
        }
    }

//TODO: add special objects from json.tools line 199
    bool ParseRapidJsonEvents::String(const Ch* str, rapidjson::SizeType size, bool copy) {
        switch (_state) {
        case Value:
            //TODO: move this out to checkSpecial so string and handle special types
            //If the string starts with $ check to see if it's a special value
            switch(str[0]) {
            case '-' :
                if (strcmp(str + 1, "Infinity") == 0) {
                    _bob->append(_field, std::numeric_limits<double>::infinity());
                    _state = Field;
                    return true;
                }
            break;
            case 'u' :
                if (strcmp(str + 1, "ndefined") == 0) {
                    _bob->appendUndefined(_field);
                    _state = Field;
                    return true;
                }
            break;
            case 'I' :
                if (strcmp(str + 1, "nfinity") == 0) {
                    _bob->append(_field, std::numeric_limits<double>::infinity());
                    _state = Field;
                    return true;
                }
            break;
            case 'N' :
                if (strcmp(str + 1, "aN") == 0) {
                    _bob->append(_field, std::numeric_limits<double>::quiet_NaN());
                    _state = Field;
                    return true;
                }
                if (strcmp(str + 1, "umberLong(") == 0) {
                    //TODO:Support NaN & -/Infinity casting in number long (-Inf w/shell for all)
                    if (str[size - 1] != ')') return false;
                    long long value;
                    if (!convertNumberLong(&value, str + 11, size - 12)) return false;
                    _bob->append(_field, value);
                    _state = Field;
                    return true;
                }
            break;
            }
            _bob->append(_field, mongo::StringData(str, size));
            _state = Field;
            return true;
        case OID:
            //Ensure that if OID is represented as a string it is the proper size for the ctor
            if (size != mongo::OID::kOIDSize) return false;
            //Ensure that the string is a hex string
            for(size_t pos = 0; pos < size; ++pos)
                if (!isxdigit(str[pos])) return false;
            _bob->append(_field, mongo::OID(str));
            _state = Unwind;
            return true;
        case Date:
            //TODO: add number long support
            return false;
        case NumberLong:
            long long value;
            if (!convertNumberLong(&value, str + 1, size - 2)) return false;
            _bob->append(_field, value);
            _state = Unwind;
            return true;
        default :
            return false;
        }
    }

} /* namespace loader */
