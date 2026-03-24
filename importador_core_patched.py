from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable
from collections import OrderedDict
import json
import re
from datetime import datetime
from io import BytesIO

import pandas as pd


DEFAULT_CONFIG = {
    "administradora_nome": "RECREDIT GARANTIDORA DE CONDOMINIOS LTDA",
    "administradora_fantasia": "RECREDIT GARANTIDORA DE CONDOMINIOS LTDA",
    "emp_codigo": "000",
    "emp_nomemp": "CONDOMINIO",
    "emp_cegece": "",
    "blq_datemi": "01/{mes}/{ano}",
    "encoding_saida": "cp1252",
    "historico_padrao": "Taxa Condominial",
    "aliases_taxas": {
        "0001": ["taxa condominial", "taxa de condominio", "cota condominial", "despesas"],
        "0002": ["fundo de reserva", "fundo de\nreserva", "fundo reserva"],
        "0003": ["gas", "consumo de gas", "gás"],
        "0004": ["agua", "água", "consumo de agua", "receita com agua"],
        "0009": ["chamada de capital", "capital parc", "capital"],
        "0022": ["produtos", "tarifas", "outros", "salao de festas", "salão de festas"],
    },
}


DATA_ROW_RE = re.compile(r"^\s*(SALA\s+\d+|SL\.\s*\d+|\d+[A-Z]?)\s+(\d{2}/\d{2}/\d{4})\s+(.+)$", re.I)
MONEY_RE = re.compile(r"-?\d[\d\.]*,\d{2}|-")
TXT_UNI_RE = re.compile(r"^(UNI_CODIGO=)(.*)$", re.M)


@dataclass
class Rateio:
    codigo: str
    historico: str
    valor: float


@dataclass
class Boleto:
    unidade: str
    vencimento: str
    historico: str = "Taxa Condominial"
    rateios: list[Rateio] = field(default_factory=list)

    def add_rateio(self, codigo: str, historico: str, valor: float):
        if valor is None or valor <= 0:
            return
        self.rateios.append(Rateio(codigo=codigo, historico=historico, valor=round(float(valor), 2)))

    @property
    def total(self) -> float:
        return round(sum(r.valor for r in self.rateios), 2)


class ImportadorRecredit:
    def __init__(self, config: dict | None = None):
        self.config = DEFAULT_CONFIG.copy()
        if config:
            self.config.update(config)
        aliases = {}
        for cod, vals in DEFAULT_CONFIG["aliases_taxas"].items():
            aliases[cod] = list(vals)
        for cod, vals in self.config.get("aliases_taxas", {}).items():
            aliases[cod] = vals
        self.config["aliases_taxas"] = aliases

    @staticmethod
    def _norm(text: str) -> str:
        rep = (
            ("á", "a"), ("à", "a"), ("ã", "a"), ("â", "a"),
            ("é", "e"), ("ê", "e"), ("í", "i"),
            ("ó", "o"), ("ô", "o"), ("õ", "o"),
            ("ú", "u"), ("ç", "c"),
        )
        s = text.lower()
        for a, b in rep:
            s = s.replace(a, b)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    @staticmethod
    def _to_money(value) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return round(float(value), 2)
        s = str(value).strip()
        if not s or s == "-" or s.lower() == "nan":
            return 0.0
        s = s.replace("R$", "").replace(" ", "")
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        try:
            return round(float(s), 2)
        except Exception:
            return None

    @staticmethod
    def _fmt_money(v: float) -> str:
        return f"{v:.2f}".replace(".", ",")

    def _codigo_por_historico(self, historico: str) -> str:
        n = self._norm(historico)
        for codigo, aliases in self.config["aliases_taxas"].items():
            for alias in aliases:
                if self._norm(alias) in n:
                    return codigo
        return "0022"


    def _extract_pdf_text(self, file_bytes: bytes) -> str:
        erros: list[str] = []

        try:
            from pypdf import PdfReader  # lazy import
            reader = PdfReader(BytesIO(file_bytes))
            pages = [(p.extract_text() or "") for p in reader.pages]
            texto = "\n\n".join(pages).strip()
            if texto:
                return texto
        except Exception as e:
            erros.append(f"pypdf: {e}")

        try:
            import pdfplumber  # lazy import
            with pdfplumber.open(BytesIO(file_bytes)) as pdf:
                pages = [(page.extract_text() or "") for page in pdf.pages]
            texto = "\n\n".join(pages).strip()
            if texto:
                return texto
        except Exception as e:
            erros.append(f"pdfplumber: {e}")

        detalhes = " | ".join(erros) if erros else "nenhum backend disponível"
        raise RuntimeError(
            "Não foi possível ler o PDF no ambiente Python atual. "
            f"Detalhes: {detalhes}. "
            "Reinstale as dependências em um ambiente virtual limpo."
        )

    def parse_pdf(self, file_bytes: bytes) -> list[Boleto]:
        full_text = self._extract_pdf_text(file_bytes)
        n = self._norm(full_text)

        if "almahcondos" in n or "residencial santa marcelina" in n or "composicao das arrecadacoes" in n:
            return self._parse_pdf_almah(full_text)
        if "simulacao das arrecadacoes" in n:
            return self._parse_pdf_jlm(full_text)
        if "composicao de cota condominial" in n:
            return self._parse_pdf_excel_consultoria(full_text)
        if "previa de cota condominial" in n or "prévia de cota condominial" in n:
            return self._parse_pdf_previa_cota(full_text)
        return []

    def _parse_pdf_jlm(self, text: str) -> list[Boleto]:
        boletos: OrderedDict[str, Boleto] = OrderedDict()
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            norm = self._norm(line)
            if any(x in norm for x in [
                "simulacao das arrecadacoes", "unidade/bloco", "cobrancas", "administradora", "@tjcondominios",
            ]):
                continue
            if re.search(r"\b\d+ de \d+\b", norm):
                continue
            m = DATA_ROW_RE.match(line)
            if not m:
                continue
            unidade, venc, resto = m.groups()
            valores = MONEY_RE.findall(resto)
            boleto = boletos.setdefault(unidade.upper().replace("  ", " "), Boleto(unidade=unidade.upper().replace("  ", " "), vencimento=venc))

            if len(valores) == 4:
                labels = [
                    ("0002", "Fundo de Reserva"),
                    ("0001", "Taxa Condominial"),
                    ("0003", "Consumo de Gás"),
                ]
                for (cod, hist), valor in zip(labels, valores[:-1]):
                    boleto.add_rateio(cod, hist, self._to_money(valor))
            elif len(valores) == 2:
                boleto.add_rateio("0009", "Chamada de Capital", self._to_money(valores[0]))
            elif len(valores) == 7:
                labels = [
                    ("0004", "Consumo de Água"),
                    ("0001", "Taxa Condominial"),
                    ("0002", "Fundo de Reserva"),
                    ("0003", "Consumo de Gás"),
                    ("0009", "Chamada de Capital"),
                    ("0022", "Salão de Festas"),
                ]
                for (cod, hist), valor in zip(labels, valores[:-1]):
                    boleto.add_rateio(cod, hist, self._to_money(valor))
        return list(boletos.values())

    def _extract_unit_before_date(self, prefix: str) -> str | None:
        m = re.search(r"(SL\.\s*\d+|SALA\s+\d+|\d+[A-Z]?)\s*$", prefix, flags=re.I)
        return m.group(1).upper().replace("  ", " ") if m else None

    def _parse_pdf_excel_consultoria(self, text: str) -> list[Boleto]:
        boletos: list[Boleto] = []
        current_block = None
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            n = self._norm(line)
            if n.startswith("bloco:"):
                current_block = line.split(":", 1)[1].strip()
                continue
            if any(x in n for x in ["composicao de cota condominial", "competencia:", "titulos", "excel consultoria", "devedor und."]):
                continue
            if re.match(r"^\d+[\.,]\d{2}$", line) or re.match(r"^\d+ titulos", n):
                continue
            md = re.search(r"(\d{2}/\d{2}/\d{4})", line)
            if not md:
                continue
            venc = md.group(1)
            before = line[:md.start()].strip()
            after = line[md.end():].strip()
            unidade = self._extract_unit_before_date(before)
            if not unidade:
                continue
            if current_block and unidade.startswith("SL"):
                unidade = unidade.replace("SL.", "SL.")
            valores = MONEY_RE.findall(after)
            if len(valores) == 3:
                labels = [("0001", "Despesas"), ("0002", "Fundo de Reserva")]
            elif len(valores) == 4:
                labels = [("0001", "Despesas"), ("0002", "Fundo de Reserva"), ("0003", "Gás")]
            elif len(valores) == 6:
                labels = [
                    ("0009", "Chamada de Capital"),
                    ("0004", "Receita com água"),
                    ("0001", "Taxa Condominial"),
                    ("0002", "Fundo de Reserva"),
                    ("0003", "Gás"),
                ]
            else:
                continue
            boleto = Boleto(unidade=unidade, vencimento=venc)
            for (cod, hist), valor in zip(labels, valores[:-1]):
                boleto.add_rateio(cod, hist, self._to_money(valor))
            boletos.append(boleto)
        return boletos


    def _parse_pdf_previa_cota(self, text: str) -> list[Boleto]:
        boletos: list[Boleto] = []
        for raw in text.splitlines():
            line = re.sub(r"\s+", " ", raw).strip()
            if not line:
                continue
            n = self._norm(line)
            if any(x in n for x in [
                "previa de cota condominial", "mes/ano:", "bloco:", "und. vencimento",
                "titulos", "títulos", "patrimony", "praiano", "receita com gas",
                "tarifa agua", "chamada de capital", "cobranca garantida", "cobrança garantida",
            ]):
                continue
            if re.match(r"^\d+[\.,]\d{2}$", line):
                continue
            m = re.match(r"^(\d+[A-Z]?|SL\.\s*\d+|SALA\s+\d+)\s+(\d{2}/\d{2}/\d{4})\s+(.+)$", line, flags=re.I)
            if not m:
                continue
            unidade, venc, resto = m.groups()
            valores = MONEY_RE.findall(resto)
            if len(valores) < 7:
                continue
            # Último valor é sempre Total; os 6 primeiros são fixos e os 3 últimos são opcionais
            pre_total = valores[:-1]
            labels_fixos = [
                ("0001", "Despesas"),
                ("0004", "Rateio Tarifa Água"),
                ("0009", "Chamada de Capital 32/36"),
                ("0022", "Cobrança Garantida"),
                ("0022", "Cotas de Capital"),
                ("0002", "Fundo de Reserva"),
            ]
            labels_opcionais = [
                ("0003", "Gás"),
                ("0022", "Receita com Gás (Desconto)"),
                ("0022", "Multa por Infração Regimental"),
            ]
            if len(pre_total) < len(labels_fixos):
                continue
            boleto = Boleto(unidade=unidade.upper().replace("  ", " "), vencimento=venc)
            for (cod, hist), valor in zip(labels_fixos, pre_total[:len(labels_fixos)]):
                boleto.add_rateio(cod, hist, self._to_money(valor))
            extras = pre_total[len(labels_fixos):]
            for (cod, hist), valor in zip(labels_opcionais, extras):
                money = self._to_money(valor)
                if money is None:
                    continue
                # Mantém descontos negativos se vierem no PDF
                boleto.rateios.append(Rateio(codigo=cod, historico=hist, valor=round(float(money), 2)))
            if boleto.rateios:
                boletos.append(boleto)
        return boletos

    def _parse_pdf_almah(self, text: str) -> list[Boleto]:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        boletos: list[Boleto] = []
        i = 0
        while i < len(lines):
            if lines[i].upper() == "ÚNICO" or lines[i].upper() == "UNICO":
                block = []
                j = i + 1
                while j < len(lines):
                    token = lines[j].strip()
                    nt = self._norm(token)
                    if token.upper() in {"ÚNICO", "UNICO"} or nt.startswith("total") or "registro(s) encontrado" in nt:
                        break
                    block.append(token)
                    j += 1
                if len(block) >= 11:
                    unidade = block[0]
                    venc = block[1]
                    nums = block[2:]
                    if re.match(r"\d{2}/\d{2}/\d{4}", venc) and len(nums) >= 9:
                        boleto = Boleto(unidade=unidade, vencimento=venc)
                        labels = [
                            ("0001", "Cota Condominial"),
                            ("0002", "Fundo de Reserva"),
                            ("0004", "Água"),
                            ("0003", "Gás"),
                            ("0022", "Produtos"),
                            ("0009", "Chamada de Capital 07/36"),
                            ("0009", "Chamada de Capital 06/36"),
                            ("0022", "Tarifas"),
                            ("0022", "Outros"),
                        ]
                        for (cod, hist), valor in zip(labels, nums[:9]):
                            boleto.add_rateio(cod, hist, self._to_money(valor))
                        boletos.append(boleto)
                i = j
            else:
                i += 1
        return boletos

    def parse_excel(self, file_bytes: bytes, filename: str = "arquivo.xlsx") -> list[Boleto]:
        df = pd.read_excel(BytesIO(file_bytes), engine="openpyxl")
        df.columns = [str(c).replace("\n", " ").strip() for c in df.columns]
        df = df.dropna(how="all")
        cols_norm = [self._norm(c) for c in df.columns]
        unidade_idx = next((i for i, c in enumerate(cols_norm) if "unid" in c or "unidade" in c), None)
        venc_idx = next((i for i, c in enumerate(cols_norm) if "venc" in c), None)
        total_idx = next((i for i, c in enumerate(cols_norm) if "total" in c), None)
        if unidade_idx is None or venc_idx is None:
            return []
        boletos = []
        for _, row in df.iterrows():
            unidade = str(row.iloc[unidade_idx]).strip()
            if not unidade or unidade.lower() in {"nan", "nat"}:
                continue
            venc_raw = row.iloc[venc_idx]
            if pd.isna(venc_raw):
                continue
            venc = None
            try:
                ts = pd.to_datetime(venc_raw, errors="coerce")
                if pd.notna(ts):
                    venc = ts.strftime("%d/%m/%Y")
            except Exception:
                venc = None
            if not venc:
                venc = str(venc_raw).strip()
                if not venc or venc.lower() in {"nan", "nat"}:
                    continue
            boleto = Boleto(unidade=unidade, vencimento=venc)
            for idx, col in enumerate(df.columns):
                if idx in {unidade_idx, venc_idx}:
                    continue
                if total_idx is not None and idx == total_idx:
                    continue
                valor = self._to_money(row.iloc[idx])
                if valor is None or valor <= 0:
                    continue
                hist = str(col)
                boleto.add_rateio(self._codigo_por_historico(hist), hist, valor)
            if boleto.rateios:
                boletos.append(boleto)
        return boletos

    def parse_file(self, file_bytes: bytes, filename: str) -> list[Boleto]:
        suffix = Path(filename).suffix.lower()
        if suffix == ".pdf":
            return self.parse_pdf(file_bytes)
        if suffix in {".xlsx", ".xlsm", ".xls"}:
            return self.parse_excel(file_bytes, filename)
        raise ValueError("Formato não suportado")

    def gerar_txt(self, boletos: Iterable[Boleto], regra_unidade: str = "padrao") -> str:
        boletos = list(boletos)
        hoje = datetime.now()
        ano = hoje.strftime("%Y")
        mes = hoje.strftime("%m")
        dia = hoje.strftime("%d")
        blq_datemi = self.config.get("blq_datemi", "01/{mes}/{ano}").format(ano=ano, mes=mes, dia=dia)

        out = []
        out.append("[leiaute]\n")
        out.append("nome=boletos_cobranca\n")
        out.append("versao=1.01\n")
        out.append(f"data_geracao={dia}/{mes}/{ano}\n")
        out.append(f"hora_geracao={hoje.strftime('%H:%M:%S')}\n")
        out.append("\n[administradora]\n")
        out.append(f"nome={self.config['administradora_nome']}\n")
        out.append(f"fantasia={self.config['administradora_fantasia']}\n")
        out.append("\n[condominio]\n")
        out.append(f"EMP_CODIGO={self.config['emp_codigo']}\n")
        out.append(f"EMP_NOMEMP={self.config['emp_nomemp']}\n")
        out.append(f"EMP_CEGECE={self.config.get('emp_cegece', '')}\n")
        out.append(f"BLQ_DATEMI={blq_datemi}\n")
        out.append(f"RECORDCOUNT={len(boletos)}\n")

        for idx, boleto in enumerate(boletos, start=1):
            out.append(f"\n[boleto_{idx}]\n")
            out.append(f"UNI_CODIGO={boleto.unidade}\n")
            out.append(f"BLQ_DATVEN={boleto.vencimento}\n")
            out.append(f"BLQ_HISTOR={boleto.historico}\n")
            out.append(f"BLQ_VLRORI={self._fmt_money(boleto.total)}\n")
            out.append(f"BLQ_VLRDES={self._fmt_money(boleto.total)}\n")
            out.append("BLQ_DESCSN=N\n")
            for j, r in enumerate(boleto.rateios, start=1):
                out.append(f"TAX_CODIGO_{j}={r.codigo}\n")
                out.append(f"RAT_HISTOR_{j}={r.historico.replace(chr(10), ' ')}\n")
                out.append(f"RAT_VLRORI_{j}={self._fmt_money(r.valor)}\n")
                out.append(f"RAT_VLRDES_{j}={self._fmt_money(r.valor)}\n")
                out.append(f"RAT_POSACO_{j}=N\n")
        txt = "".join(out)
        return aplicar_regra_unidade_txt(txt, regra_unidade)


def carregar_config_json(file_bytes: bytes | None) -> dict:
    if not file_bytes:
        return {}
    return json.loads(file_bytes.decode("utf-8"))


def regra_portal_uni(valor: str) -> str:
    m = re.match(r"^(\d+)\s+(\d+)$", valor.strip())
    if m:
        unidade, bloco = m.groups()
        return f"B{int(bloco)}-{unidade}"
    return valor


def regra_mafalda_uni(valor: str) -> str:
    m = re.match(r"^(\d+)-(\d+)$", valor.strip())
    if m:
        bloco, apto = m.groups()
        return f"{apto} {bloco}"
    return valor


def regra_orlando_txt(texto: str, codigo_alvo: str = "101") -> str:
    matches = re.findall(r"UNI_CODIGO=(\d+)", texto)
    letra_atual = "@"
    def inc(letra: str) -> str:
        return "A" if letra == "Z" else chr(ord(letra) + 1)
    for match in matches:
        if match == codigo_alvo:
            letra_atual = inc(letra_atual)
        texto = re.sub(fr"UNI_CODIGO={re.escape(match)}", f"UNI_CODIGO={letra_atual}-{match}", texto, count=1)
    return texto


def aplicar_regra_unidade_txt(texto: str, regra: str, codigo_alvo_orlando: str = "101") -> str:
    regra = (regra or "padrao").lower()
    if regra == "padrao":
        return texto
    if regra == "portal":
        return TXT_UNI_RE.sub(lambda m: f"{m.group(1)}{regra_portal_uni(m.group(2))}", texto)
    if regra == "mafalda":
        return TXT_UNI_RE.sub(lambda m: f"{m.group(1)}{regra_mafalda_uni(m.group(2))}", texto)
    if regra == "orlando":
        return regra_orlando_txt(texto, codigo_alvo_orlando)
    return texto
