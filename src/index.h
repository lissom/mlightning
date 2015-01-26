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

#include <algorithm>
#include <assert.h>
#include <deque>
#include <tuple>
#include <utility>

namespace tools {

    /*
     * Compare function so that the Key can be used by any desired compare function
     */
    template<typename Cmp, typename Key>
    struct IndexPairCompare {
        IndexPairCompare(Cmp cmp) : compare(std::move(cmp)) { }

        template<typename IndexDataType>
        bool operator()(const IndexDataType& lhs, const IndexDataType& rhs) const {
            static_assert(std::is_same<typename IndexDataType::first_type, Key>::value, "Need to have the same key types to compare");
            return compare(lhs.first, rhs.first);
        }

        template<typename IndexDataType>
        bool operator()(const Key& lhs, const IndexDataType& rhs) const {
            static_assert(std::is_same<typename IndexDataType::first_type, Key>::value, "Need to have the same key types to compare");
            return compare(lhs, rhs.first);
        }

        template<typename IndexDataType>
        bool operator()(const IndexDataType& lhs, const Key& rhs) const {
            static_assert(std::is_same<typename IndexDataType::first_type, Key>::value, "Need to have the same key types to compare");
            return compare(lhs.first, rhs);
        }

        friend std::ostream& operator<<(std::ostream& out, const IndexPairCompare& cmp) {
            out << cmp.compare;
            return out;
        }

        //NOT const.  The container can declare this class const if so desired
        Cmp compare;
    };

    /*
     * Key = index
     * Tll = logical location
     * Compare = bool operator()(Key, Key) for sorting.
     *
     * This template is a manually sorted index.  The use case is inserting data that the ordering
     * is only cared about at key points.
     *
     * Modeled around a stl container
     */
    template<typename Key, typename Value, typename Compare>
    class BasicIndex {
    public:
        using Data = typename std::pair<Key, Value>;
        using Container = typename std::deque<Data>;
        using iterator = typename Container::iterator;
        using const_iterator = typename Container::const_iterator;
        using CompareHolder = IndexPairCompare<Compare, Key>;

        BasicIndex(BasicIndex &&rhs) :
                _compare(std::move(rhs._compare)), _container(std::move(rhs._container))
        {
        }
        template<typename ...Args>
        BasicIndex(const BasicIndex& bi, Args&&... args) :
                _compare(CompareHolder(Cmp(std::forward<Args>(args)...))), _container(bi)
        {
        }
        template<typename ...Args>
        BasicIndex(Args ... args) :
                _compare(CompareHolder(Compare(args...)))
        {
        }
        explicit BasicIndex(Compare compare) :
                _compare(CompareHolder(std::move(compare)))
        {
        }

        friend std::ostream& operator<<(std::ostream& o, const BasicIndex::iterator& i) {
            o << i->first << "::" << i->second;
            return o;
        }

        const_iterator cbegin() const {
            return _container.cbegin();
        }
        const_iterator cend() const {
            return _container.cend();
        }
        iterator begin() {
            return _container.begin();
        }
        iterator end() {
            return _container.end();
        }
        Value& front() {
            return _container.front().second;
        }
        Value& back() {
            return _container.back().second;
        }
        void clear() {
            _container.clear();
        }
        void sort() {
            std::sort(_container.begin(), _container.end(), _compare);
        }
        size_t size() const {
            return _container.size();
        }

        Container& container() {
            return _container;
        }

        const Container& container() const {
            return _container;
        }

        Compare getCompare() const {
            return _compare.compare;
        }

        //TODO: Change to &&
        /**
         * Moves into the container in an unordered fashion, must be sorted afterward if desired
         */
        void insertUnordered(Key key, Value&& value) {
            _container.emplace_back(std::make_pair(std::move(key), std::forward<Value>(value)));
        }

        /**
         * Moves a range into the container in an unordered fashion, must be sorted afterward if desired
         */
        template<typename InputIterator>
        void insertUnordered(InputIterator first, InputIterator last) {
            static_assert(std::is_constructible<typename std::iterator_traits<InputIterator>::value_type, Data>::value, "Cannot construct insert type");
            _container.insert(end(), first, last);
        }

        /**
         * Moves from the given index to this one.
         */
        void steal(BasicIndex& takeFrom) {
            insertUnordered(takeFrom.begin(), takeFrom.end());
            takeFrom.clear();
        }

        /**
         * Sorts the data
         */
        void finalize() {
            sort();
        }

        /**
         * Assumes that a sort has taken place
         */
        iterator find(const Key& key) {
            auto i = std::lower_bound(begin(), end(), key, _compare);
            //TODO: remove assert, right now it's here because this should never be true
            assert(i->first == key);
            //Ensure positive infinity returns the correct result
            return i;

        }

        /**
         * Assumes that a sort has taken place
         */
        Value& at(const Key& key) {
            auto i = find(key);
            if (i == end()) throw std::range_error("Index out of bounds");
            return i->second;
        }

        bool empty() {
            return _container.empty();
        }

        /*
         * Assumes sort has taken place
         * Assumes upper bounded includes infinity, so nothing is ever returned at end.
         */
        Value& upperBoundSafe(const Key& key) {
            auto i = std::upper_bound(begin(), end(), key, _compare);
            //Ensure positive infinity returns the correct result
            if (i == _container.end()) --i;
            return i->second;
        }

        /**
         * Assumes that a sort has taken place
         */
        Value& upperBound(const Key& key) {
            auto i = std::upper_bound(begin(), end(), key, _compare);
            return i->second;
        }

        /**
         * Assumes that a sort has taken place
         */
        Value& lowerBound(const Key& key) {
            auto i = std::lower_bound(begin(), end(), key, _compare);
            return i->second;
        }

        const CompareHolder& compare() {
            return _compare;
        }

    private:

        CompareHolder _compare;
        Container _container;
    };

    template<typename Key, typename Value, typename Cmp = std::less<Key>> using Index = BasicIndex<Key, Value, Cmp>;

}  //namespace tools
