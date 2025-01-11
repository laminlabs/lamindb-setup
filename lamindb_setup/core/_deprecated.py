from __future__ import annotations

# BSD 3-Clause License
# Copyright (c) 2017-2018 P. Angerer, F. Alexander Wolf, Theis Lab
# All rights reserved.
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import warnings
from functools import wraps


def deprecated(new_name: str):
    """Deprecated.

    This is a decorator which can be used to mark functions, methods and properties
    as deprecated. It will result in a warning being emitted
    when the function is used.

    It will also hide the function from the docs.

    Example::

        @property
        @deprecated("n_files")
        def n_objects(self) -> int:
            return self.n_files

    """

    def decorator(func):
        @wraps(func)
        def new_func(*args, **kwargs):
            warnings.warn(
                f"Use {new_name} instead of {func.__name__}, "
                f"{func.__name__} will be removed in the future.",
                category=FutureWarning,
                stacklevel=2,
            )
            return func(*args, **kwargs)

        setattr(new_func, "__deprecated", True)
        return new_func

    return decorator
