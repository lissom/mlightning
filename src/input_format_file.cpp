/*
 * inputformat.tools
 *
 *  Created on: Aug 17, 2014
 *      Author: charlie
 */

#include "input_format_file.h"

#include <assert.h>
#include <iostream>
#include "input_types.h"

namespace loader {

    const bool InputFormatJson::_registerFactory = InputFormatFactory::registerCreator(JSON_INPUT,
                                                                           &InputFormatJson::create);
    const bool InputFormatBson::_registerFactory = InputFormatFactory::registerCreator(BSON_INPUT,
                                                                               &InputFormatBson::create);

    void InputFormatJson::reset(tools::LocSegment segment)
    {
        /*
         * If there is a begin point, scroll to the start of the next line
         * This ensures we don't read a partial object/an object another thread is
         */
        _locSegment = std::move(segment);
        if (_infile.is_open())
            _infile.close();
        _infile.open(_locSegment.file, std::ios_base::in);
        /*
         * If we are starting somewhere other than the very start of the file, scroll to the
         * end of that line and start there.  Prevents partial documents and races.
         */
        //TODO: test this on windows, what happens on a CR-LF if you're on LF: fails to read one?
        if (_locSegment.begin) {
            _infile.seekg(_locSegment.begin);
            std::string string;
            getline(_infile,string);
        }
        assert(_infile.is_open());
    }

    //TODO: Keep average object size and implement fromjson with a large/smaller buffer
    bool InputFormatJson::next(mongo::BSONObj* const nextDoc) {
        ++_lineNumber;
        if (_locSegment.end && (_infile.tellg() > _locSegment.end)) return false;
        if (!getline(_infile, _line)) return false;
        //*nextDoc = mongo::fromjson(_line);/*
        rapidjson::StringStream ss(_line.c_str());
        _events.reset();
        _events.bufferSizeSet(_bufferSize);
        if(!_reader.Parse(ss, _events)) {
            rapidjson::ParseErrorCode error = _reader.GetParseErrorCode();
            size_t offset = _reader.GetErrorOffset();
            std::cerr << "Error file: " << _locSegment.file << ":" << _locSegment.begin << " line #:"
                    << _lineNumber << rapidjson::GetParseError_En(error) << "\nLine: " << _line
                    << "\noffset: " << offset << " near " << _line.substr(offset, 10) << "..."
                    << std::endl;
            return false;
        }
        *nextDoc = _events.obj();
        _bufferSize = nextDoc->objsize();
        //*/
        return true;
    }

    void InputFormatBson::reset(tools::LocSegment segment)
    {
        _locSegment = std::move(segment);
        if (_infile.is_open())
            _infile.close();
        _infile.open(_locSegment.file, std::ios_base::in);
        //we currently don't handle partial bson
        assert(_locSegment.begin == 0);
        assert(_locSegment.end == 0);
        assert(_infile.is_open());
    }

    bool InputFormatBson::next(mongo::BSONObj* const nextDoc) {
        //Read the size into the buffer
        ++_docCount;
        //bsonspec.org defines the size of a bson object as 32 bit integer
        int32_t bsonSize;
        _infile.read(_buffer.data(), sizeof(bsonSize));
        if (_infile.eof())
            return false;
        if (!_infile) {
            std::cerr << "Failed reading file: " << _locSegment.file
                    << ".  Reading size of object: " << _docCount
                    << std::endl;
            exit(EXIT_FAILURE);
        }
        //TODO: Undefined, endian, see mongo/src/mongo/platform/endian.h
        bsonSize = *reinterpret_cast<int32_t*>(_buffer.data());
        if (bsonSize > mongo::BSONObjMaxUserSize) {
            std::cerr << "Size too large for object in file: " << _locSegment.file
                    << ".  Reading object: " << _docCount << ".  Size: " << bsonSize
                    << std::endl;
            exit(EXIT_FAILURE);
        } else if (bsonSize == 0) {
            std::cerr << "Size too small for object in file: " << _locSegment.file
                    << ".  Reading object: " << _docCount << ".  Size: " << bsonSize
                    << std::endl;
            exit(EXIT_FAILURE);
        }
        //Read the rest of the object in the buffer
        _infile.read(_buffer.data() + (sizeof(bsonSize)), bsonSize - sizeof(bsonSize));
        if (!_infile) {
            std::cerr << "Failed reading file: " << _locSegment.file
                    << ".  Reading object: " << _docCount
                    << std::endl;
            exit(EXIT_FAILURE);
        }
        mongo::BSONObj tmpObj(_buffer.data());
        *nextDoc = tmpObj.copy();
        return true;
    }


}  //namespace loader
