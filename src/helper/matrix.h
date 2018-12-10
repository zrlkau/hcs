// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef __helper__matrix_h
#define __helper__matrix_h

#include "helper/macros.h"
#include "logger.h"
#include <vector>

template <typename T> class Matrix
{
  public:
    Matrix() : _x_dim(0), _y_dim(0)
    {
    }

    Matrix(size_t x_dim, size_t y_dim) : _x_dim(x_dim), _y_dim(y_dim)
    {
        _data.resize(x_dim * y_dim);
    }

    void resize(size_t x_dim, size_t y_dim, bool keep = false)
    {
        //        // We only support growth!
        //        assert(x_dim >= this->_x_dim && y_dim >= this->_y_dim);

        _data.resize(x_dim * y_dim);
        _data.clear();
        /*
	if (keep && x_dim >= this->_x_dim && y_dim >= this->_y_dim) {
	    // We need to preserve the data at the corresponding x/y coordinates.
	    if (this->_x_dim > 0 && this->_y_dim > 0) {
		for (ssize_t x = this->_x_dim - 1; x >= 0; x--) {
		    for (ssize_t y = this->_y_dim - 1; y >= 0; y--) {
			_data[(x * y_dim) + y] = _data[(x * this->_y_dim) + y];
		    }
		}
	    }
	}
*/
        this->_x_dim = x_dim;
        this->_y_dim = y_dim;
    }

    void clear()
    {
        this->_data.clear();
    }

    T &operator()(size_t x, size_t y)
    {
        return _data[(x * _y_dim) + y];
    }

    const T &operator()(size_t x, size_t y) const
    {
        return _data[(x * _y_dim) + y];
    }

    void set(size_t x, size_t y, T value)
    {
        _data[(x * _y_dim) + y] = value;
    }

    const T get(size_t x, size_t y) const
    {
        return _data[(x * _y_dim) + y];
    }

    GET(size_t, x_dim);
    GET(size_t, y_dim);

    size_t size1() const
    {
        return this->_x_dim;
    }
    size_t size2() const
    {
        return this->_y_dim;
    }

  private:
    std::vector<T> _data;
    size_t         _x_dim;
    size_t         _y_dim;
};

std::ostream &operator<<(std::ostream &os, const Matrix<uint64_t> &obj);
std::ostream &operator<<(std::ostream &os, const Matrix<int> &obj);
std::ostream &operator<<(std::ostream &os, const Matrix<short> &obj);
std::ostream &operator<<(std::ostream &os, const Matrix<bool> &obj);

#endif
