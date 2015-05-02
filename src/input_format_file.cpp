/*    Copyright Charlie Page 2014
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#include <assert.h>
#include "input_format_file.h"
#include <iostream>
#include "loader_defs.h"
#include <string.h>
#include "stream_writer.h"

namespace loader {

const bool InputFormatJson::_registerFactory = InputFormatFactory::registerCreator(INPUT_JSON,
        &InputFormatJson::create);
const bool InputFormatBson::_registerFactory = InputFormatFactory::registerCreator(INPUT_BSON,
        &InputFormatBson::create);
const bool InputFormatMltn::_registerFactory = InputFormatFactory::registerCreator(INPUT_MLTN,
        &InputFormatMltn::create);

void InputFormatJson::reset(tools::LocSegment segment) {
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
        getline(_infile, string);
    }
    assert(_infile.is_open());
}

//TODO: Keep average object size and implement fromjson with a large/smaller buffer
bool InputFormatJson::next(mongo::BSONObj* const nextDoc) {
    ++_lineNumber;
    if (_locSegment.end && (_infile.tellg() > _locSegment.end))
        return false;
    if (!getline(_infile, _line))
        return false;
    rapidjson::StringStream ss(_line.c_str());
    _events.reset();
    _events.bufferSizeSet(_bufferSize);
    if (!_reader.Parse(ss, _events)) {
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

void InputFormatBson::reset(tools::LocSegment segment) {
    _locSegment = std::move(segment);
    if (_infile.is_open())
        _infile.close();
    _infile.open(_locSegment.file, std::ios_base::in);
    //we currently don't handle partial bson
    assert(_locSegment.begin == 0);
    assert(_locSegment.end == 0);
    assert(_infile.is_open());
    _docCount = 0;
}

bool InputFormatBson::next(mongo::BSONObj* const nextDoc) {
    //Using index 1 for error reporting
    ++_docCount;
    //Read the size into the buffer
    BsonSize bsonSize;
    _infile.read(_buffer.data(), sizeof(bsonSize));
    if (_infile.eof())
        return false;
    if (!_infile) {
        std::cerr << "Failed reading file: " << _locSegment.file << ".  Reading size of object: "
                << _docCount << std::endl;
        exit(EXIT_FAILURE);
    }
    //TODO: Undefined, endian, see mongo/src/mongo/platform/endian.h
    bsonSize = *reinterpret_cast<int32_t*>(_buffer.data());
    if (bsonSize > mongo::BSONObjMaxUserSize) {
        std::cerr << "Size too large for doc in file: " << _locSegment.file << ".  Reading doc #: "
                << _docCount << ".  Size: " << bsonSize << std::endl;
        exit(EXIT_FAILURE);
    } else if (bsonSize < 1) {
        std::cerr << "Size too small for doc in file: " << _locSegment.file << ".  Reading doc #: "
                << _docCount << ".  Size: " << bsonSize << std::endl;
        exit(EXIT_FAILURE);
    }
    //Read the rest of the object in the buffer
    _infile.read(_buffer.data() + (sizeof(bsonSize)), bsonSize - sizeof(bsonSize));
    if (!_infile) {
        std::cerr << "Failed reading from file: " << _locSegment.file << ".  Reading doc #: "
                << _docCount << std::endl;
        exit(EXIT_FAILURE);
    }
    mongo::BSONObj tmpObj(_buffer.data());
    *nextDoc = tmpObj.copy();
    return true;
}

void InputFormatMltn::reset(tools::LocSegment segment) {
    std::ifstream infile;
    _locSegment = std::move(segment);
    if (infile.is_open())
        infile.close();
    infile.open(_locSegment.file, std::ios_base::in);
    //we currently don't handle partial bson
    assert(_locSegment.begin == 0);
    assert(_locSegment.end == 0);
    assert(infile.is_open());
    FileChunkType blockType;
    SequenceId sid;
    _bufferSize = readFromStream(infile, &blockType, &sid, &_buffer);
    assert(_bufferSize);
    assert(blockType == FileChunkType::data);
    assert(sid == 0);
    _docCount = 0;
    _bufferPos = 0;
}

bool InputFormatMltn::next(mongo::BSONObj* const nextDoc) {
    //Check to see if the end has been reached
    if (_bufferPos == _bufferSize)
        return false;

    ++_docCount;
    BsonSize bsonSize;
    //TODO: Change to just pointing the bson object and try...catch over size
    memcpy(&bsonSize, &_buffer[_bufferPos], sizeof(bsonSize));
    if (bsonSize > mongo::BSONObjMaxUserSize) {
        std::cerr << "Size too large for doc in block from file: " << _locSegment.file
                << ".  Reading object #: " << _docCount << ".  Size: " << bsonSize << std::endl;
        exit(EXIT_FAILURE);
    } else if (bsonSize < 1) {
        std::cerr << "Size too small for doc in block from file: " << _locSegment.file
                << ".  Reading object: " << _docCount << ".  Size: " << bsonSize << std::endl;
        exit(EXIT_FAILURE);
    }
    //Read the rest of the object in the buffer
    if (_bufferPos + bsonSize > _bufferSize) {
        std::cerr << "Failed reading in block from file: " << _locSegment.file
                << ".  Reading doc #: " << _docCount << std::endl;
        exit(EXIT_FAILURE);
    }
    mongo::BSONObj tmpObj(&_buffer[_bufferPos]);
    *nextDoc = tmpObj.copy();
    _bufferPos += bsonSize;
    return true;
}
}  //namespace loader
