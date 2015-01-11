/*
 * inputformat.h
 *
 *  Created on: Aug 17, 2014
 *      Author: charlie
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
        std::string _line;
        unsigned long long _docCount{};
        tools::LocSegment _locSegment;
        std::vector<char> _buffer;

        const static bool _registerFactory;

        ParseRapidJsonEvents _events;
        size_t _bufferSize{};

        size_t buffersize() { return _bufferSize; }
    };

}  //namespace loader
