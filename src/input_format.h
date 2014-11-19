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

    class AbstractFileInputFormat;

    /**
     * Pointer returned by factory functions
     */
    using InputFormatPointer = std::unique_ptr<AbstractFileInputFormat>;

    /**
     * Factory function signature
     */
    using CreateInputFormatFunction =
            std::function<InputFormatPointer(void)>;

    /*
     * Factory
     */
    using InputFormatFactory = tools::RegisterFactory<InputFormatPointer,
            CreateInputFormatFunction>;

    /*
     * Public interface for the extraction of documents from sources.
     * bool next(mongo::BSONObj* nextDoc) is used to allow for the greatest variety of input sources
     */
    class AbstractFileInputFormat {
    public:
        virtual ~AbstractFileInputFormat() { }

        virtual void reset(tools::LocSegment segment) = 0;
        /**
         * If there is a document available, this function places the next one in the passed
         * variable
         * @return returns true if there is document available. False otherwise.
         */
        virtual bool next(mongo::BSONObj* nextDoc) = 0;

        /**
         * Returns the position of the document.  Should assert if such a thing isn't possible and
         * this is called.
         */
        virtual size_t pos() = 0;
    };

    /**
     * Reads JSON from a file.
     */
    class InputFormatJson : public AbstractFileInputFormat {
    public:
        InputFormatJson() { };
        virtual void reset(tools::LocSegment segment);
        virtual bool next(mongo::BSONObj* nextDoc);
        virtual size_t pos() {
            return _infile.tellg();
        }

        static InputFormatPointer create() {
            return InputFormatPointer(new InputFormatJson());
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
    class InputFormatBson : public AbstractFileInputFormat {
    public:
        InputFormatBson() : _buffer(mongo::BSONObjMaxUserSize) {};
        virtual void reset(tools::LocSegment segment);
        virtual bool next(mongo::BSONObj* nextDoc);
        virtual size_t pos() {
            return _infile.tellg();
        }

        static InputFormatPointer create() {
            return InputFormatPointer(new InputFormatBson());
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
