Gestion de params : 
logique métier à valider dans TransformValidator => éventuellement aligner la structure du YAML avec le prérequis => ensemble des configs à ajouter dans les fonctions de validations 

Séparation des responsabilités entre config et processors : 
Quelque chose du style : 

func NewTransformProcessor(cfg ProcessorConfig) (Processor, error) {
    return &TransformProcessor{
        logger:    cfg.logger,
        fieldName: cfg.Config["field_name"].(string),
        operation: cfg.Config["operation"].(string),
        params:    cfg.Config["params"].(map[string]interface{}),
    }, nil
}


Je peux retirer les signatures d'erreur de mes constructeurs par contre ma factory doit le conserver car elle gère le cas du type inconnu

Ajouter le testing des processors pour faire des tests de comportement (ex: test de la logique métier de transformation) et pas seulement des tests de validation de config

// Ce qui vaut la peine d'être testé

// TransformProcessor
- uppercase("hello") → "HELLO"
- add_prefix("world", "foo_") → "foo_world"
- un champ absent du message → message retourné inchangé

// DropProcessor
- message dont le champ matche le critère → retourne nil
- message dont le champ ne matche pas → retourne le message intact

// EnrichProcessor
- le champ est bien ajouté au message avec la bonne valeur
- un champ existant est-il écrasé ou protégé ?

// TimestampReplayProcessor
- offset de +1h → timestamp correctement décalé
- target_timestamp → timestamp remplacé par la valeur exacte