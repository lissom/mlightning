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

#pragma once

#include <fstream>
#include <functional>
#include <memory>
#include "factory.h"
#include "mongo_cxxdriver.h"
#include "parserapidjsonevents.h"
#include "tools.h"

namespace loader {

    /*
     * Iterface to fetch from a stream
     * bool next(mongo::BSONObj* nextDoc) is used to allow for the greatest variety of input sources
     */
    class FileInputInterface {
    public:
        virtual ~FileInputInterface() {}
        virtual void reset(tools::LocSegment segment) = 0;
        /**
         * If there is a document available, this function places the next one in the passed
         * variable
         * @return returns true if there is document available. False otherwise.
         */
        virtual bool next(mongo::BSONObj* const nextDoc) = 0;
        /**
         * Returns the position of the document.
         */
        virtual size_t pos() = 0;
    };

    /**
     * Pointer returned by factory functions
     */
    using FileInputInterfacePtr = std::unique_ptr<FileInputInterface>;

    /**
     * Factory function signature
     */
    using CreateFileInputFunction =
            std::function<FileInputInterfacePtr(void)>;

    /*
     * Factory
     */
    using InputFormatFactory = tools::RegisterFactory<FileInputInterfacePtr,
            CreateFileInputFunction>;

    /**
     * Reads JSON from a file.
     */
    class InputFormatJson : public FileInputInterface {
    public:
        InputFormatJson() { };
        virtual void reset(tools::LocSegment segment);
        virtual bool next(mongo::BSONObj* const nextDoc);
        virtual size_t pos() {
            return _infile.tellg();
        }

        static FileInputInterfacePtr create() {
            return FileInputInterfacePtr(new InputFormatJson());
        }

    private:
        std::ifstream _infile;
        std::string _line;
        rapidjson::Reader _reader;
        //line number is one indexed
        unsigned long long _lineNumber{};
        tools::LocSegment _locSegment;

        const static bool _registerFactory;

        ParseRapidJsonEvents _events;
        size_t _bufferSize{};

        size_t buffersize() { return _bufferSize; }
    };

    /**
     * Reads BSON from a file.
     */
    class InputFormatBson : public FileInputInterface {
    public:
        InputFormatBson() : _buffer(mongo::BSONObjMaxUserSize) {};
        virtual void reset(tools::LocSegment segment);
        virtual bool next(mongo::BSONObj* const nextDoc);
        virtual size_t pos() {
            return _infile.tellg();
        }

        static FileInputInterfacePtr create() {
            return FileInputInterfacePtr(new InputFormatBson());
        }

    private:
        std::ifstream _infile;
        //The document offset for error reporting
        unsigned long long _docCount{};
        tools::LocSegment _locSegment;
        std::vector<char> _buffer;

        const static bool _registerFactory;
    };

    /**
     * Reads for an mLightning formatted file
     */
    //TODO: use Source/Sink from snappy for less coping
    class InputFormatMltn : public FileInputInterface {
    public:
        InputFormatMltn() {}
        virtual void reset(tools::LocSegment segment);
        virtual bool next(mongo::BSONObj* const nextDoc);
        virtual size_t pos() {
            return _bufferPos;
        }

        static FileInputInterfacePtr create() {
            return FileInputInterfacePtr(new InputFormatMltn());
        }

    private:
        //The document offset for error reporting
        unsigned long long _docCount{};
        tools::LocSegment _locSegment;
        std::vector<char> _buffer;
        size_t _bufferSize{};

        const static bool _registerFactory;

        size_t _bufferPos{};
    };

}  //namespace loader
