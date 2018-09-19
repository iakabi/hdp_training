package formation.sparkbatch;

import java.io.Serializable;

public class Personne implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private int idPersonne;
	private String nom;
	private String prenom;
	private String cdCivil;
	private String dtNaissance;
	private String cdSituFam;
	private String cdFamProf;
	private String cdSectActiv;
	private String anneesEmploi;
	private String email;
	private double scoreCredit;
	
	public Personne() {
		this.idPersonne = -1;
	}
	
	public Personne(int idPersonne, String nom, String prenom, String cdCivil, String dtNaissance, String cdSituFam,
			String cdFamProf, String cdSectActiv, String anneesEmploi, String email, double scoreCredit) {
		super();
		this.idPersonne = idPersonne;
		this.nom = nom;
		this.prenom = prenom;
		this.cdCivil = cdCivil;
		this.dtNaissance = dtNaissance;
		this.cdSituFam = cdSituFam;
		this.cdFamProf = cdFamProf;
		this.cdSectActiv = cdSectActiv;
		this.anneesEmploi = anneesEmploi;
		this.email = email;
		this.scoreCredit = scoreCredit;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}


	public void setIdPersonne(int idPersonne) {
		this.idPersonne = idPersonne;
	}

	public void setNom(String nom) {
		this.nom = nom;
	}

	public void setPrenom(String prenom) {
		this.prenom = prenom;
	}

	public void setCdCivil(String cdCivil) {
		this.cdCivil = cdCivil;
	}

	public void setDtNaissance(String dtNaissance) {
		this.dtNaissance = dtNaissance;
	}

	public void setCdSituFam(String cdSituFam) {
		this.cdSituFam = cdSituFam;
	}

	public void setCdFamProf(String cdFamProf) {
		this.cdFamProf = cdFamProf;
	}

	public void setCdSectActiv(String cdSectActiv) {
		this.cdSectActiv = cdSectActiv;
	}

	public void setAnneesEmploi(String anneesEmploi) {
		this.anneesEmploi = anneesEmploi;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public void setScoreCredit(double scoreCredit) {
		this.scoreCredit = scoreCredit;
	}

	public int getIdPersonne() {
		return idPersonne;
	}


	public String getNom() {
		return nom;
	}


	public String getPrenom() {
		return prenom;
	}


	public String getCdCivil() {
		return cdCivil;
	}


	public String getDtNaissance() {
		return dtNaissance;
	}


	public String getCdSituFam() {
		return cdSituFam;
	}


	public String getCdFamProf() {
		return cdFamProf;
	}


	public String getCdSectActiv() {
		return cdSectActiv;
	}


	public String getAnneesEmploi() {
		return anneesEmploi;
	}


	public String getEmail() {
		return email;
	}


	public double getScoreCredit() {
		return scoreCredit;
	}	
	
	public String toString() {
		return "Personne [idPersonne=" + idPersonne + ", nom=" + nom + ", prenom=" + prenom + ", cdCivil=" + cdCivil
				+ ", dtNaissance=" + dtNaissance + ", cdSituFam=" + cdSituFam + ", cdFamProf=" + cdFamProf
				+ ", cdSectActiv=" + cdSectActiv + ", anneesEmploi=" + anneesEmploi + ", email=" + email
				+ ", scoreCredit=" + scoreCredit + "]";
	}
	
}
