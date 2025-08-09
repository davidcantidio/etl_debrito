#!/usr/bin/env python3
"""
Script para converter épicos existentes para formato compatível com TDD.

Este script:
1. Lê arquivos de épico JSON
2. Quebra tasks grandes em micro-ciclos TDD (red-green-refactor)
3. Reorganiza deliverables (testes primeiro)
4. Adiciona campos específicos de TDD
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any


def create_tdd_microtasks(task: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Converte uma task grande em micro-tasks seguindo ciclo TDD.
    
    Args:
        task: Task original do épico
        
    Returns:
        Lista de micro-tasks TDD (red-green-refactor)
    """
    microtasks = []
    base_id = task['id']
    
    # Extrair comportamentos testáveis da descrição e critérios
    behaviors = extract_testable_behaviors(task)
    
    for idx, behavior in enumerate(behaviors):
        # Red phase - escrever teste que falha
        microtasks.append({
            "id": f"{base_id}.{idx * 3 + 1}",
            "title": f"TEST: {behavior['test_name']}",
            "tdd_phase": "red",
            "estimate_minutes": 5,
            "description": f"Escrever teste que verifica: {behavior['description']}",
            "test_specs": [behavior['test_name']],
            "deliverables": [f"tests/test_{behavior['module']}.py::{behavior['test_name']}"],
            "acceptance_criteria": [
                "Teste falha com assertion correta",
                "Mensagem de erro é clara e específica"
            ]
        })
        
        # Green phase - implementar código mínimo
        microtasks.append({
            "id": f"{base_id}.{idx * 3 + 2}",
            "title": f"IMPL: {behavior['feature']}",
            "tdd_phase": "green",
            "estimate_minutes": 8,
            "description": f"Implementar código mínimo para passar o teste",
            "test_specs": [behavior['test_name']],
            "deliverables": [
                f"tests/test_{behavior['module']}.py::{behavior['test_name']}",
                behavior['implementation_file']
            ],
            "acceptance_criteria": [
                "Teste passa com implementação mínima",
                "Nenhum teste anterior quebra"
            ]
        })
        
        # Refactor phase (a cada 3 comportamentos)
        if (idx + 1) % 3 == 0:
            microtasks.append({
                "id": f"{base_id}.{idx * 3 + 3}",
                "title": f"REFACTOR: {behavior['module']}",
                "tdd_phase": "refactor",
                "estimate_minutes": 10,
                "description": "Melhorar design mantendo todos os testes verdes",
                "test_specs": ["all_tests_still_pass"],
                "deliverables": ["refactored code with green tests"],
                "acceptance_criteria": [
                    "Código mais limpo e maintível",
                    "Todos os testes continuam verdes",
                    "Complexidade ciclomática reduzida"
                ]
            })
    
    return microtasks


def extract_testable_behaviors(task: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Extrai comportamentos testáveis de uma task.
    
    Args:
        task: Task original
        
    Returns:
        Lista de comportamentos testáveis
    """
    behaviors = []
    
    # Analisar deliverables para identificar módulos
    modules = []
    for deliverable in task.get('deliverables', []):
        if deliverable.endswith('.py') and not deliverable.startswith('tests/'):
            module_name = Path(deliverable).stem
            modules.append(module_name)
    
    # Se não houver módulos, usar ID da task
    if not modules:
        modules = [f"module_{task['id'].replace('.', '_')}"]
    
    # Gerar comportamentos baseados nos critérios de aceitação
    for idx, criteria in enumerate(task.get('acceptance_criteria', [])):
        for module in modules:
            behaviors.append({
                'test_name': f"test_{module}_should_{sanitize_test_name(criteria)}",
                'description': criteria,
                'feature': f"{module} {criteria.lower()[:30]}",
                'module': module,
                'implementation_file': f"transform/{module}.py"
            })
    
    # Se não houver critérios, criar comportamento genérico
    if not behaviors:
        module = modules[0]
        behaviors.append({
            'test_name': f"test_{module}_basic_functionality",
            'description': task.get('description', 'Basic functionality'),
            'feature': task.get('title', 'Basic feature'),
            'module': module,
            'implementation_file': f"transform/{module}.py"
        })
    
    return behaviors[:3]  # Limitar a 3 comportamentos por task


def sanitize_test_name(text: str) -> str:
    """Converte texto em nome válido para teste."""
    # Remove caracteres especiais e converte para snake_case
    import re
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    text = text.lower().replace(' ', '_')
    return text[:40]  # Limitar tamanho


def convert_epic_to_tdd(epic_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converte um épico inteiro para formato TDD.
    
    Args:
        epic_data: Dados do épico original
        
    Returns:
        Épico convertido para TDD
    """
    tdd_epic = epic_data.copy()
    epic = tdd_epic['epic']
    
    # Adicionar metadados TDD
    epic['tdd_enabled'] = True
    epic['methodology'] = 'Test-Driven Development'
    
    # Converter todas as tasks
    all_microtasks = []
    for task in epic['tasks']:
        # Pular tasks de documentação/análise
        if any(keyword in task['title'].lower() for keyword in ['mapear', 'analisar', 'verificar', 'documentar']):
            # Manter task original para análise
            task['tdd_skip_reason'] = 'Analysis/documentation task'
            all_microtasks.append(task)
        else:
            # Converter para micro-tasks TDD
            microtasks = create_tdd_microtasks(task)
            all_microtasks.extend(microtasks)
    
    epic['tasks'] = all_microtasks
    
    # Atualizar checklist do épico
    epic['checklist_epic_level'].insert(0, "Todos os testes escritos antes da implementação")
    epic['checklist_epic_level'].insert(1, "100% de cobertura de testes nos novos módulos")
    epic['checklist_epic_level'].insert(2, "Ciclo red-green-refactor seguido consistentemente")
    
    # Adicionar automation hooks para TDD
    epic['automation_hooks']['test_runner'] = 'pytest'
    epic['automation_hooks']['coverage_threshold'] = 90
    epic['automation_hooks']['pre_commit_hooks'] = ['pytest', 'coverage']
    
    return tdd_epic


def main():
    """Função principal."""
    if len(sys.argv) < 2:
        print("Uso: python convert_to_tdd.py <arquivo_epico.json>")
        sys.exit(1)
    
    input_file = Path(sys.argv[1])
    if not input_file.exists():
        print(f"Erro: Arquivo {input_file} não encontrado")
        sys.exit(1)
    
    # Ler épico original
    with open(input_file, 'r', encoding='utf-8') as f:
        epic_data = json.load(f)
    
    # Converter para TDD
    tdd_epic = convert_epic_to_tdd(epic_data)
    
    # Salvar versão TDD
    output_file = input_file.with_stem(f"{input_file.stem}_tdd")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(tdd_epic, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Épico convertido para TDD salvo em: {output_file}")
    print(f"📊 Tasks originais: {len(epic_data['epic']['tasks'])}")
    print(f"📊 Micro-tasks TDD: {len(tdd_epic['epic']['tasks'])}")


if __name__ == "__main__":
    main()