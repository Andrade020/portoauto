# -*- coding: utf-8 -*-
import base64
import pandas as pd

class GeradorRelatorioHTML:
    """
    classe de gerar relatórios HTML com um design pré-definido.
    pode:> aceitar diferentes seções e dados.
    """
    def __init__(self, titulo_relatorio, subtitulo, data_relatorio, logo_path):
        self.titulo_relatorio = titulo_relatorio
        self.subtitulo = subtitulo
        self.data_relatorio = data_relatorio
        self.logo_path = logo_path
        self.html_parts = []
        self._inicializar_html()

    def _encode_logo(self):
        try:
            with open(self.logo_path, "rb") as image_file:
                return base64.b64encode(image_file.read()).decode('utf-8')
        except FileNotFoundError:
            print(f"[AVISO] Arquivo de logo não encontrado em: {self.logo_path}")
            return None

    def _get_css(self):
        # O CSS do seu relatório original foi copiado para cá.
        # Você pode facilmente alterar a paleta de cores aqui.
        return """
        <style>
            :root {
                --cor-primaria: #163f3f;
                --cor-secundaria: #76c6c5;
                --cor-fundo: #f9f9f9;
                --cor-texto: #313131;
                --cor-header-texto: #FFFFFF;
                --cor-borda-detalhe: #76c6c5;
            }
            body { font-family: "Gill Sans MT", Arial, sans-serif; background-color: var(--cor-fundo); color: var(--cor-texto); margin: 0; padding: 0; }
            .main-content { padding: 25px; }
            header { background-color: var(--cor-primaria); color: var(--cor-header-texto); padding: 20px 40px; display: flex; justify-content: space-between; align-items: center; border-bottom: 5px solid var(--cor-secundaria); }
            header .logo img { height: 75px; }
            header .report-title h1, header .report-title h2, header .report-title h3 { margin: 0; padding: 0; font-weight: normal; }
            header .report-title h1 { font-size: 1.6em; }
            header .report-title h2 { font-size: 1.4em; color: #d0d0d0; }
            header .report-title h3 { font-size: 1.1em; color: #a0a0a0; }
            .container-botoes { display: flex; flex-wrap: wrap; gap: 15px; margin-bottom: 25px; }
            details { flex: 1 1 100%; border: 1px solid var(--cor-secundaria); border-radius: 8px; overflow: hidden; margin-top: 15px; }
            summary { font-size: 1.1em; font-weight: bold; color: var(--cor-header-texto); background-color: var(--cor-primaria); padding: 15px 20px; cursor: pointer; outline: none; list-style-type: none; }
            summary:hover { background-color: #0e5d5f; }
            details[open] summary { background-color: var(--cor-secundaria); color: var(--cor-texto); }
            summary::before { content: '► '; margin-right: 8px; font-size: 0.8em;}
            details[open] summary::before { content: '▼ '; }
            .content-wrapper { padding: 20px; background-color: #FFFFFF; }
            table.dataframe { border-collapse: collapse; width: 100%; }
            table.dataframe th, table.dataframe td { border: 1px solid #bbbbbb; text-align: left; padding: 10px; vertical-align: middle; }
            table.dataframe th { background-color: var(--cor-primaria); color: var(--cor-header-texto); }
            table.dataframe tr:nth-child(even) { background-color: #eeeeee; }
            footer { background-color: #f0f0f0; color: #555555; font-size: 0.8em; padding: 25px 40px; margin-top: 40px; border-top: 1px solid #dddddd; text-align: center; }
        </style>
        """

    def _inicializar_html(self):
        self.html_parts.append("<!DOCTYPE html><html lang='pt-BR'><head>")
        self.html_parts.append(f"<meta charset='UTF-8'><title>{self.titulo_relatorio}</title>")
        self.html_parts.append(self._get_css())
        self.html_parts.append("</head><body>")

        # --- Cabeçalho ---
        self.html_parts.append("<header>")
        self.html_parts.append(f"""
        <div class="report-title">
            <h1>{self.titulo_relatorio}</h1>
            <h2>{self.subtitulo}</h2>
            <h3>{self.data_relatorio}</h3>
        </div>
        """)
        logo_base64 = self._encode_logo()
        if logo_base64:
            self.html_parts.append(f'<div class="logo"><img src="data:image/png;base64,{logo_base64}" alt="Logo"></div>')
        self.html_parts.append("</header>")
        self.html_parts.append("<div class='main-content'>")

    def adicionar_secao_colapsavel(self, titulo, descricao, dataframe, formatters=None, aberto=False):
        """Adiciona uma seção <details> que pode ser expandida/recolhida."""
        open_tag = "open" if aberto else ""
        self.html_parts.append(f"<details {open_tag}>")
        self.html_parts.append(f'<summary title="{descricao}">{titulo}</summary>')
        self.html_parts.append("<div class='content-wrapper'>")
        if dataframe is not None and not dataframe.empty:
            df_html = dataframe.to_html(index=False, classes='dataframe', formatters=formatters, na_rep='-', escape=False)
            self.html_parts.append(df_html)
        else:
            self.html_parts.append(f"<p>{descricao}</p>")
        self.html_parts.append("</div></details>")

    def salvar(self, output_path):
        """Salva o conteúdo HTML gerado em um arquivo."""
        # --- Rodapé ---
        self.html_parts.append("</div>") # Fim do main-content
        self.html_parts.append("<footer>")
        self.html_parts.append(f'<p>Relatório gerado em {self.data_relatorio}. © Sua Empresa</p>')
        self.html_parts.append("</footer>")
        self.html_parts.append("</body></html>")

        final_html = "\n".join(self.html_parts)
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(final_html)
            print(f"Relatório salvo com sucesso em: {output_path}")
        except Exception as e:
            print(f"[ERRO] Falha ao salvar o relatório: {e}")