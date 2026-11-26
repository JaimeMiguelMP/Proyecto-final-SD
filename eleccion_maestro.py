class EleccionMaestro:
    """Elección por prioridad de ID sobre el conjunto visible de nodos."""

    def __init__(self, nodo, miembros, fallos_para_caida=2):
        self.nodo = nodo
        self.maestro = None
        self.fallos_para_caida = fallos_para_caida
        self.fallos = dict.fromkeys(miembros, 0)

    def observar(self, alcanzables):
        for nid in self.fallos:
            self.fallos[nid] = 0 if nid in alcanzables else self.fallos[nid] + 1
        candidatos = set(alcanzables) | {self.nodo}
        if self.maestro is not None and self.fallos[self.maestro] < self.fallos_para_caida:
            candidatos.add(self.maestro)
        self.maestro = max(candidatos)
        return self.maestro

    @property
    def caidos(self):
        return {nid for nid, fallos in self.fallos.items() if fallos >= self.fallos_para_caida}
