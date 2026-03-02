package bindings

const BlobBindingType BindingType = "blobTrigger"
const BlobInputType BindingType = "blob"

// BlobBinding is the JSON representation for Blob Trigger.
type BlobBinding struct {
	Path       string `json:"path"`
	Connection string `json:"connection"`
}

// Blob is the user-facing configuration for a Blob trigger.
type Blob struct {
	Name       string
	Path       string
	Connection string
}

func (b *Blob) GetBindingType() BindingType { return BlobBindingType }

func (b *Blob) ToBinding() Binding {
	return Binding{
		Name:      b.Name,
		Type:      string(b.GetBindingType()),
		Direction: "in",
		BlobBinding: &BlobBinding{
			Path:       b.Path,
			Connection: b.Connection,
		},
	}
}

// BlobInput allows reading from a blob.
type BlobInput struct {
	Name       string
	Path       string
	Connection string
}

func (b *BlobInput) GetBindingType() BindingType { return BlobInputType }

func (b *BlobInput) ToBinding() Binding {
	return Binding{
		Name:      b.Name,
		Type:      string(b.GetBindingType()),
		Direction: "in",
		BlobBinding: &BlobBinding{
			Path:       b.Path,
			Connection: b.Connection,
		},
	}
}

// BlobOutput allows writing to a blob.
type BlobOutput struct {
	Name       string
	Path       string
	Connection string
}

func (b *BlobOutput) GetBindingType() BindingType { return BlobInputType } // Uses same type "blob"

func (b *BlobOutput) ToBinding() Binding {
	return Binding{
		Name:      b.Name,
		Type:      string(b.GetBindingType()),
		Direction: "out",
		BlobBinding: &BlobBinding{
			Path:       b.Path,
			Connection: b.Connection,
		},
	}
}
