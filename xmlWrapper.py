from collections import Mapping



class ETreeDictWrapper(Mapping):

    def __init__(self, elem, attr_prefix = '@', list_tags = ()):
        self.elem = elem
        self.attr_prefix = attr_prefix
        self.list_tags = list_tags

    def _wrap(self, e):
        if isinstance(e, basestring):
            return e
        if len(e) == 0 and len(e.attrib) == 0:
            return e.text
        return type(self)(
            e,
            attr_prefix = self.attr_prefix,
            list_tags = self.list_tags,
        )

    def __getitem__(self, key):
        if key.startswith(self.attr_prefix):
            return self.elem.attrib[key[len(self.attr_prefix):]]
        else:
            subelems = [ e for e in self.elem.iterchildren() if e.tag == key ]
            if len(subelems) > 1 or key in self.list_tags:
                return [ self._wrap(x) for x in subelems ]
            elif len(subelems) == 1:
                return self._wrap(subelems[0])
            else:
                raise KeyError(key)

    def __iter__(self):
        return iter(set( k.tag for k in self.elem) |
                    set( self.attr_prefix + k for k in self.elem.attrib ))

    def __len__(self):
        return len(self.elem) + len(self.elem.attrib)

    # defining __contains__ is not necessary, but improves speed
    def __contains__(self, key):
        if key.startswith(self.attr_prefix):
            return key[len(self.attr_prefix):] in self.elem.attrib
        else:
            return any( e.tag == key for e in self.elem.iterchildren() )