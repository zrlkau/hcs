// Authors: Michael Kaufmann <kau@zurich.ibm.com>
//
// Copyright (c) 2018, IBM Corporation, All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

#ifndef helper_macros_h
#define helper_macros_h

#define COMMA ,

#define SETGET(type, var)         \
    inline void var(type val)     \
    {                             \
        this->_##var = val;       \
    }                             \
    inline const type var() const \
    {                             \
        return this->_##var;      \
    }                             \
    inline type var()             \
    {                             \
        return this->_##var;      \
    }

#define VSETGET(container, type, var)         \
    inline void var(size_t idx, type val)     \
    {                                         \
        this->_##var[idx] = val;              \
    }                                         \
    inline const type &var(size_t idx) const  \
    {                                         \
        return this->_##var[idx];             \
    }                                         \
    inline type &var(size_t idx)              \
    {                                         \
        return this->_##var[idx];             \
    }                                         \
    inline const container<type> &var() const \
    {                                         \
        return this->_##var;                  \
    }                                         \
    inline container<type> &var()             \
    {                                         \
        return this->_##var;                  \
    }

#define ASETGET(type, var)          \
    inline void var(type val)       \
    {                               \
        this->_##var.store(val);    \
    }                               \
    inline const type var() const   \
    {                               \
        return this->_##var.load(); \
    }                               \
    inline type var()               \
    {                               \
        return this->_##var.load(); \
    }
#define ADDGET(type, var)            \
    inline void var(type val)        \
    {                                \
        this->_##var.push_back(val); \
    }                                \
    inline const type var() const    \
    {                                \
        return this->_##var;         \
    }                                \
    inline type var()                \
    {                                \
        return this->_##var;         \
    }

#define SET(type, var)        \
    inline void var(type val) \
    {                         \
        this->_##var = val;   \
    }
#define GET(type, var)            \
    inline const type var() const \
    {                             \
        return this->_##var;      \
    }                             \
    inline type var()             \
    {                             \
        return this->_##var;      \
    }
#define GETPTR(type, var)          \
    inline const type *var() const \
    {                              \
        return &this->_##var;      \
    }                              \
    inline type *var()             \
    {                              \
        return &this->_##var;      \
    }

#define CGET(type, var)           \
    inline const type var() const \
    {                             \
        return this->_##var;      \
    }

#endif
