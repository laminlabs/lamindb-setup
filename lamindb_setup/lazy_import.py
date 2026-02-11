"""helpers for providing lazy imports of submodules."""

import ast
import contextlib
import os
import sys
from collections.abc import Callable, Iterator, Sequence
from importlib import import_module
from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec, SourceFileLoader
from importlib.util import (
    find_spec,
    module_from_spec,
    spec_from_file_location,
    spec_from_loader,
)
from pathlib import Path
from types import ModuleType, new_class
from typing import Any, TypedDict

__all__ = [
    "enable_lazy_imports",
    "disable_lazy_imports",
]

LAZY_PROXY_ATTR = "__lazy_proxy__"


_ImportableNames = tuple[
    list[str],
    list[str],
    list[str],
    dict[str, tuple[str | None, str, int]],
]


def get_importable_names(file_path: str) -> _ImportableNames:
    """Return names of top-level classes, functions, and variables in a Python file."""
    with open(file_path, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=file_path)
    class_names = []
    func_names = []
    var_names = []
    from_imports: dict[str, tuple[str | None, str, int]] = {}
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            # collect top-level class definitions
            class_names.append(node.name)
        elif isinstance(node, ast.FunctionDef):
            # collect top-level function definitions
            func_names.append(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    if isinstance(node.value, ast.Name):
                        # for direct aliases, assign appropriately
                        if node.value.id in class_names:
                            class_names.append(target.id)
                        elif node.value.id in func_names:
                            func_names.append(target.id)
                        elif node.value.id in from_imports:
                            from_imports[target.id] = from_imports[node.value.id]
                        else:
                            var_names.append(target.id)
                    else:
                        var_names.append(target.id)
                elif isinstance(target, ast.Tuple | ast.List):
                    for elt in target.elts:
                        if isinstance(elt, ast.Name):
                            var_names.append(elt.id)
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "*":
                    continue  # skip star imports
                from_imports[alias.asname or alias.name] = (
                    node.module,
                    alias.name,
                    node.level,
                )
    return class_names, func_names, var_names, from_imports


def iter_submodule_specs(fullname: str) -> Iterator[ModuleSpec]:
    """Iterate over submodule names of a given module."""
    spec = find_spec(fullname)
    if spec is None:
        raise ModuleNotFoundError(f"module {fullname!r} not found")
    if spec.submodule_search_locations:
        for locations in spec.submodule_search_locations:
            if not Path(locations).is_dir():
                continue
            for dirpath, _dirnames, filenames in os.walk(locations):
                for filename in sorted(filenames):
                    if not filename.endswith(".py"):
                        continue

                    a_pth = Path(dirpath, filename)
                    r_pth = a_pth.relative_to(locations)
                    if filename == "__init__.py":
                        submodule_fullname = ".".join([fullname, *r_pth.parts[:-1]])
                        submodule_search_locations = [str(a_pth.parent)]
                    else:
                        submodule_fullname = ".".join(
                            [fullname, *r_pth.parts[:-1], r_pth.stem]
                        )
                        submodule_search_locations = None
                    loader = SourceFileLoader(submodule_fullname, str(a_pth))
                    s = spec_from_file_location(
                        submodule_fullname,
                        str(a_pth),
                        loader=loader,
                        submodule_search_locations=submodule_search_locations,
                    )
                    if s is not None:
                        yield s
    elif spec.origin:
        yield spec


def import_module_from_spec(spec: ModuleSpec) -> ModuleType:
    """Import a module from a given ModuleSpec."""
    mod = module_from_spec(spec)
    sys.modules[mod.__name__] = mod
    if spec.loader:
        spec.loader.exec_module(mod)
    return mod


OnLoadCallable = Callable[[str, str], None]


class _ProxyMeta(type):
    """MetaClass to enable lazy import of classes."""

    __lazy_on_load__: OnLoadCallable | None = None
    __lazy_spec__: ModuleSpec

    def _eager_load_cls(cls) -> Any:
        try:
            mod = sys.modules[cls.__module__]
        except KeyError:
            eager_load = True
            mod = None
        else:
            eager_load = hasattr(mod, LAZY_PROXY_ATTR)
        if eager_load or mod is None:
            if cls.__lazy_on_load__:
                cls.__lazy_on_load__(cls.__module__, cls.__name__)
            with disable_lazy_imports():
                mod = import_module_from_spec(cls.__lazy_spec__)
        return getattr(mod, cls.__name__)

    def __repr__(cls) -> str:
        try:
            mod = sys.modules[cls.__module__]
        except KeyError:
            proxy_repr = True
            mod = None
        else:
            proxy_repr = hasattr(mod, LAZY_PROXY_ATTR)
        if proxy_repr or mod is None:
            return f"<proxy-class {cls.__module__}.{cls.__name__}>"
        else:
            return repr(getattr(mod, cls.__name__))

    def __call__(cls, *args, **kwargs):
        return cls._eager_load_cls()(*args, **kwargs)

    def __subclasshook__(cls, __subclass):
        return issubclass(__subclass, cls._eager_load_cls())

    def __getattr__(cls, item):
        return getattr(cls._eager_load_cls(), item)


def new_metaclass_proxy(
    name: str, module: str, *, spec: ModuleSpec, on_load: OnLoadCallable | None
) -> Any:
    """Create a metaclass proxy for module level classes."""
    ns_dct = {
        "__module__": module,
        "__qualname__": name,
        "__lazy_on_load__": staticmethod(on_load) if on_load else None,
        "__lazy_spec__": spec,
    }
    return new_class(name, (), {"metaclass": _ProxyMeta}, lambda ns: ns.update(ns_dct))


def new_callable_proxy(
    name: str, module: str, *, spec: ModuleSpec, on_load: OnLoadCallable | None = None
) -> Any:
    """Create a callable proxy for module level functions."""

    def func(*args, **kwargs):
        try:
            mod = sys.modules[module]
        except KeyError:
            eager_load = True
            mod = None
        else:
            eager_load = hasattr(mod, LAZY_PROXY_ATTR)
        if eager_load or mod is None:
            if on_load:
                on_load(module, name)
            with disable_lazy_imports():
                mod = import_module_from_spec(spec)
        return getattr(mod, name)(*args, **kwargs)

    func.__module__ = module
    func.__name__ = func.__qualname__ = name
    return func


class LazyProxyLoader(Loader):
    """Loader to enable lazy import of submodules."""

    def __init__(self, spec: ModuleSpec, on_load: OnLoadCallable | None = None) -> None:
        self.spec = spec
        self.on_load = on_load

    def create_module(self, spec: ModuleSpec) -> ModuleType | None:
        return ModuleType(spec.name)

    def exec_module(self, module: ModuleType) -> None:
        if self.spec.origin:
            classes, functions, variables, from_imports = get_importable_names(
                self.spec.origin
            )
        else:
            classes = functions = variables = []
            from_imports = {}

        def __getattr__(item):
            if item in from_imports:
                from_module, item_name, level = from_imports[item]
                if level == 0:
                    module_name = from_module
                elif self.spec.submodule_search_locations is not None:
                    module_name = f"{module.__name__}.{from_module}"
                else:
                    module_name = (
                        f"{module.__name__.rsplit('.', level)[0]}.{from_module}"
                    )
                mod = import_module(module_name)
                return getattr(mod, item_name)
            if item in classes:
                return new_metaclass_proxy(
                    item, module=module.__name__, spec=self.spec, on_load=self.on_load
                )
            elif item in functions:
                return new_callable_proxy(
                    item, module=module.__name__, spec=self.spec, on_load=self.on_load
                )
            elif item in variables:
                raise NotImplementedError("fixme: handle variables in lazy import")
            else:
                raise AttributeError(
                    f"Module '{module.__name__}' has no attribute '{item}'"
                )

        module.__dict__["__getattr__"] = __getattr__
        setattr(module, LAZY_PROXY_ATTR, True)


class LazyProxyFinder(MetaPathFinder):
    """MetaPathFinder to enable lazy import of specific submodules."""

    enabled: bool = True

    def __init__(self, *prefixes: str, on_load: OnLoadCallable | None = None) -> None:
        self.prefixes = prefixes
        self.on_load = on_load
        self.submodules: dict[str, ModuleSpec] = {
            spec.name: spec
            for prefix in prefixes
            for spec in iter_submodule_specs(prefix)
        }

    def __repr__(self):
        return f"<{self.__class__.__module__}.{self.__class__.__name__} prefixes={self.prefixes!r} enabled={self.enabled} at 0x{hex(id(self))}>"

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None,
        target: ModuleType | None = None,
    ) -> ModuleSpec | None:
        if self.enabled and fullname in self.submodules:
            spec = self.submodules[fullname]
            is_package = spec.submodule_search_locations is not None
            return spec_from_loader(
                fullname,
                LazyProxyLoader(spec, on_load=self.on_load),
                is_package=is_package,
            )
        return None


def enable_lazy_imports(
    prefix: str,
    /,
    *prefixes: str,
    on_load: OnLoadCallable | None = None,
) -> None:
    """register lazy import mechanism."""
    all_prefixes = {prefix, *prefixes}

    if loaded := all_prefixes.intersection(sys.modules):
        for module_name in sorted(loaded, reverse=True):
            try:
                mod = sys.modules[module_name]
            except KeyError:
                pass
            else:
                if hasattr(mod, LAZY_PROXY_ATTR):
                    del sys.modules[module_name]

    for finder in sys.meta_path:
        if isinstance(finder, LazyProxyFinder) and all_prefixes == set(finder.prefixes):
            continue
    else:
        sys.meta_path.insert(0, LazyProxyFinder(*all_prefixes, on_load=on_load))


@contextlib.contextmanager
def disable_lazy_imports(*, restore_on_error: bool = True) -> Iterator[None]:
    """disable lazy import mechanism."""
    # disable all lazy finders and remove their submodules from sys.modules
    for finder in sys.meta_path:
        if isinstance(finder, LazyProxyFinder):
            finder.enabled = False
            submodules = set(sys.modules).intersection(finder.submodules)
            for m in sorted(submodules, reverse=True):
                try:
                    mod = sys.modules[m]
                except KeyError:
                    pass
                else:
                    if hasattr(mod, LAZY_PROXY_ATTR):
                        del sys.modules[m]
    try:
        yield
    except:
        if restore_on_error:
            for finder in sys.meta_path:
                if isinstance(finder, LazyProxyFinder):
                    finder.enabled = True
        raise
