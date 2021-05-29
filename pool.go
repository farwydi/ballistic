package ballistic

type Pool interface {
	Append(models []DataModel) error
	Push(model DataModel) error
	Eject(limit int) (models []DataModel, err error)
}
