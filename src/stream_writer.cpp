/*
 * streamwriter.cpp
 *
 *  Created on: Apr 17, 2015
 *      Author: charlie
 */

#include "stream_writer.h"

namespace loader {

    void metaToStream(std::ostream& out, const FileChunkType blockType,
            const SequenceId sequenceId, const FileChunkFormat formatType, const uint64_t size) {
        out << static_cast<std::underlying_type<FileChunkType>::type>(blockType)
            << sequenceId
            << static_cast<std::underlying_type<FileChunkFormat>::type>(formatType)
            << size;
    }

    void metaFromStream(std::istream& in, FileChunkType* const blockType,
                SequenceId* const sequenceId, FileChunkFormat* const formatType, uint64_t* const size) {
        std::underlying_type<FileChunkType>::type fileChunkHeaderType;
        std::underlying_type<FileChunkFormat>::type fileChunkFormatType;
        in >> fileChunkHeaderType
            >> *sequenceId
            >> fileChunkFormatType
            >> *size;
        *blockType = static_cast<FileChunkType>(fileChunkHeaderType);
        *formatType = static_cast<FileChunkFormat>(fileChunkFormatType);

    }

    void writeToStream(std::ostream& out, const FileChunkType blockType,
            const SequenceId sequenceId, const FileChunkFormat formatType, const mongo::BufBuilder& data) {
        std::string compressed;
        switch(formatType) {
        case FileChunkFormat::none :
            metaToStream(out, blockType, sequenceId, formatType, data.len());
            out.write(data.buf(), data.len());
            break;
        case FileChunkFormat::snappy :
            if (snappy::Compress(data.buf(), data.len(), &compressed) >
                size_t(data.len() / 9 * 10)) {
                writeToStream(out, blockType, sequenceId, FileChunkFormat::none, data);
                break;
            }
            metaToStream(out, blockType, sequenceId, formatType, compressed.size());
            out.write(&compressed[0], compressed.size());
            break;
        default :
            throw std::range_error("Invalid compression type in writeToStream");
        }
    }

    size_t readFromStream(std::istream& in, FileChunkType* blockType, SequenceId* sequenceId,
            std::vector<char>* data) {
        uint64_t size;
        FileChunkFormat compressionType;
        metaFromStream(in, blockType, sequenceId, &compressionType, &size);
        std::vector<char> raw;
        switch (static_cast<FileChunkFormat>(compressionType)) {
        case FileChunkFormat::none :
            data->reserve(size);
            in.read(&(*data)[0], size);
            return size;
        case FileChunkFormat::snappy :
            raw.reserve(size);
            in.read(&raw[0], size);
            if(!snappy::IsValidCompressedBuffer(&raw[0],size))
                throw std::logic_error("Invalid compression buffer with snappy");
            size_t uncompressdSize;
            snappy::GetUncompressedLength(&raw[0], size, &uncompressdSize);
            data->reserve(uncompressdSize);
            if(!snappy::RawUncompress(&raw[0], size, &(*data)[0]))
                throw std::logic_error("Unable to decompress block with snappy");
            return uncompressdSize;
        default : throw std::range_error("readFromStream: Unknown compression type");
        }
    }

} /* namespace loader */
