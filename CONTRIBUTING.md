# Contribuindo para o projeto

Obrigado por dedicar seu tempo para melhorar este repositório! As instruções abaixo descrevem como preparar o ambiente, executar as checagens locais e garantir que o *pipeline* de Continuous Integration (CI) seja aprovado na primeira tentativa.

---

## 1. Configurar o ambiente de desenvolvimento

```bash
# clone o repositório
git clone https://github.com/<org>/<repo>.git
cd <repo>

# crie e ative o ambiente virtual
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# instale dependências de runtime e de desenvolvimento
pip install -r requirements.txt          # dependências de produção
pip install -r requirements-dev.txt      # ferramentas de lint, testes, etc.
```

> **Dica:** mantenha o *venv* ativado sempre que trabalhar no projeto – todas as ferramentas de lint ficarão disponíveis na linha de comando.

---

## 2. CI / pré‑commit

O repositório usa **[pre‑commit](https://pre-commit.com/)** para garantir consistência de código antes de cada *commit* e para replicar exatamente as mesmas verificações executadas no CI.

### 2.1 Instalar hooks de pré‑commit

```bash
pre-commit install            # instala os hooks no .git/hooks/
```

A partir desse momento, cada `git commit` executará automaticamente todos os checadores abaixo apenas nos arquivos alterados.

Caso deseje rodar todos os hooks contra o código inteiro (o que é útil antes de abrir uma *pull request*), execute:

```bash
pre-commit run --all-files
```

### 2.2 Checadores configurados

| Ferramenta        | Objetivo                                                  | Como rodar manualmente   |
| ----------------- | --------------------------------------------------------- | ------------------------ |
| **Flake8**        | Estilo & erros estáticos (PEP‑8, bugbear, comprehensions) | `flake8 .`               |
| **isort**         | Ordenação de imports                                      | `isort .`                |
| **nbqa + flake8** | Lint em Jupyter Notebooks (`*.ipynb`)                     | `nbqa flake8 notebooks/` |
| **black**         | Formatação automática de código                           | `black .`                |

> Os comandos acima são idempotentes: rodá‑los localmente **deve** resultar em zero erros. Se isso acontecer, o mesmo código certamente passará no *pipeline* de CI.

---

## 3. Fluxo recomendado de trabalho

1. **Crie um branch** descritivo: `git checkout -b feat/melhorias-datas`
2. Faça suas alterações e adicione testes sempre que possível.
3. Execute `pre-commit run --all-files` para garantir que tudo passa.
4. `git commit -am "feat: adiciona tratamento de datas no relatório X"`
5. `git push` e abra sua *pull request*.

---

## 4. Dúvidas ou problemas

Caso encontre dificuldades para rodar qualquer etapa, abra uma *issue* ou pergunte no canal da equipe. Manter o mesmo conjunto de verificações local/CI é fundamental para um fluxo de contribuição tranquilo.

Boa contribuição! ✨
