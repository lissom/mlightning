/*
 * Pipeline.h
 *
 *  Created on: Nov 1, 2014
 *      Author: charlie
 */

#pragma once

namespace tools {

    template< typename in, typename out>
    class Pipeline {
    public:
        enum class State { active, pause, complete };

        State state() { return _state; }
        void pressure(int pressure) { _pressure = pressure; }

    private:
        State _state;
        int _pressure;
    };

} /* namespace tools */

