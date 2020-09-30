package br.edu.utfpr.dv.sireata.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import br.edu.utfpr.dv.sireata.model.Ata;
import br.edu.utfpr.dv.sireata.model.Ata.TipoAta;
import br.edu.utfpr.dv.sireata.util.DateUtils;

@RestController
@RequestMapping("/Repository/Ata")
public class AtaDAO {


		@Autowired
		private Ata ata;   //Repositório de Atas

		@GetMapping("/{id}")
		public Repository.Ata buscarAta (@PathVariable Long id) {
			return ata.findOne(id);
		}

		// ----------------------------------------Resolver problemas de conflito

		@GetMapping
			public Repository.Ata buscarPorNumero(@PathVariable Repository.Ata pauta) {
			return ata.findOne(id);
		}

		@GetMapping
		public Repository.Ata buscarPorNumero(@PathVariable Repository.Ata int idPauta) {
		return ata.findOne(id);
		}

		@GetMapping
		public Repository.Ata buscarPorCampus(@PathVariable Repository.Ata campus) {
		return ata.findOne(id);
		}

		@GetMapping
		public Repository.Ata buscarPorOrgao(@PathVariable Repository.Ata orgao) {
		return ata.findOne(id);
		}

		@GetMapping
		public Repository.Ata buscarPorDepartamento(@PathVariable Repository.Ata departamento) {
		return ata.findOne(id);
	}

		@GetMapping
		public List<Repository.Ata> pesquisar() {
			return produtos.findAll();
		}

		@PostMapping
		public Repository.Ata salvar(@RequestBody Ata ata) {
			return ata.save(ata);
		}

		@DeleteMapping("/{id}")
		public void deletar(@PathVariable Long id) {
			ata.delete(id);
		}




//------------------------------------------------------------------------------metodos a serem estudados a melhor forma de implementacao




	public int buscarProximoNumeroAta(int idOrgao, int ano, TipoAta tipo) throws SQLException{
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		try{
			conn = ConnectionDAO.getInstance().getConnection();
			stmt = conn.prepareStatement(
				"SELECT MAX(numero) AS numero FROM atas WHERE idOrgao = ? AND YEAR(data) = ? AND tipo = ?");
		
			stmt.setInt(1, idOrgao);
			stmt.setInt(2, ano);
			stmt.setInt(3, tipo.getValue());
			
			rs = stmt.executeQuery();
			
			if(rs.next()){
				return rs.getInt("numero") + 1;
			}else{
				return 1;
			}
		}finally{
			if((rs != null) && !rs.isClosed())
				rs.close();
			if((stmt != null) && !stmt.isClosed())
				stmt.close();
			if((conn != null) && !conn.isClosed())
				conn.close();
		}
	}
	


	public List<Ata> listarPublicadas() throws SQLException{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		try{
			conn = ConnectionDAO.getInstance().getConnection();
			stmt = conn.createStatement();
		
			rs = stmt.executeQuery("SELECT atas.*, orgaos.nome AS orgao, p.nome AS presidente, s.nome AS secretario " +
				"FROM atas INNER JOIN orgaos ON orgaos.idOrgao=atas.idOrgao " +
				"INNER JOIN departamentos ON departamentos.idDepartamento=orgaos.idDepartamento " +
				"INNER JOIN usuarios p ON p.idUsuario=atas.idPresidente " +
				"INNER JOIN usuarios s ON s.idUsuario=atas.idSecretario " +
				"WHERE atas.publicada=1 ORDER BY atas.data DESC");
		
			List<Ata> list = new ArrayList<Ata>();
			
			while(rs.next()){
				list.add(this.carregarObjeto(rs));
			}
			
			return list;
		}finally{
			if((rs != null) && !rs.isClosed())
				rs.close();
			if((stmt != null) && !stmt.isClosed())
				stmt.close();
			if((conn != null) && !conn.isClosed())
				conn.close();
		}
	}
	

	

	
}
		public List<Ata> listarNaoPublicadas(int idUsuario) throws SQLException{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		try{
			conn = ConnectionDAO.getInstance().getConnection();
			stmt = conn.createStatement();
		
			rs = stmt.executeQuery("SELECT DISTINCT atas.*, orgaos.nome AS orgao, p.nome AS presidente, s.nome AS secretario " +
				"FROM atas INNER JOIN orgaos ON orgaos.idOrgao=atas.idOrgao " +
				"INNER JOIN departamentos ON departamentos.idDepartamento=orgaos.idDepartamento " +
				"INNER JOIN usuarios p ON p.idUsuario=atas.idPresidente " +
				"INNER JOIN usuarios s ON s.idUsuario=atas.idSecretario " +
				"INNER JOIN ataparticipantes ON ataparticipantes.idAta=atas.idAta " +
				"WHERE atas.publicada=0 AND ataparticipantes.idUsuario=" + String.valueOf(idUsuario) +" ORDER BY atas.data DESC");
		
			List<Ata> list = new ArrayList<Ata>();
			
			while(rs.next()){
				list.add(this.carregarObjeto(rs));
			}
			
			return list;
		}finally{
			if((rs != null) && !rs.isClosed())
				rs.close();
			if((stmt != null) && !stmt.isClosed())
				stmt.close();
			if((conn != null) && !conn.isClosed())
				conn.close();
		}
	}
	

	


	public void publicar(int idAta, byte[] documento) throws SQLException{
		Connection conn = null;
		PreparedStatement stmt = null;
		
		try{
			conn = ConnectionDAO.getInstance().getConnection();
			stmt = conn.prepareStatement("UPDATE atas SET documento=?, dataPublicacao=?, publicada=1, aceitarComentarios=0 WHERE publicada=0 AND idAta=?");
		
			stmt.setBytes(1, documento);
			stmt.setTimestamp(2, new java.sql.Timestamp(DateUtils.getNow().getTime().getTime()));
			stmt.setInt(3, idAta);
			
			stmt.execute();
		}finally{
			if((stmt != null) && !stmt.isClosed())
				stmt.close();
			if((conn != null) && !conn.isClosed())
				conn.close();
		}
	}
	
	public void liberarComentarios(int idAta) throws SQLException{
		Connection conn = null;
		Statement stmt = null;
		
		try{
			conn = ConnectionDAO.getInstance().getConnection();
			stmt = conn.createStatement();
		
			stmt.execute("UPDATE atas SET aceitarComentarios=1 WHERE publicada=0 AND idAta=" + String.valueOf(idAta));
		}finally{
			if((stmt != null) && !stmt.isClosed())
				stmt.close();
			if((conn != null) && !conn.isClosed())
				conn.close();
		}
	}
	
	public void bloquearComentarios(int idAta) throws SQLException{
		Connection conn = null;
		Statement stmt = null;
		
		try{
			conn = ConnectionDAO.getInstance().getConnection();
			stmt = conn.createStatement();
		
			stmt.execute("UPDATE atas SET aceitarComentarios=0 WHERE idAta=" + String.valueOf(idAta));
		}finally{
			if((stmt != null) && !stmt.isClosed())
				stmt.close();
			if((conn != null) && !conn.isClosed())
				conn.close();
		}
	}
	
	private Ata carregarObjeto(ResultSet rs) throws SQLException{
		Ata ata = new Ata();
		
		ata.setIdAta(rs.getInt("idAta"));
		ata.getOrgao().setIdOrgao(rs.getInt("idOrgao"));
		ata.getOrgao().setNome(rs.getString("orgao"));
		ata.getPresidente().setIdUsuario(rs.getInt("idPresidente"));
		ata.getPresidente().setNome(rs.getString("presidente"));
		ata.getSecretario().setIdUsuario(rs.getInt("idSecretario"));
		ata.getSecretario().setNome(rs.getString("secretario"));
		ata.setTipo(TipoAta.valueOf(rs.getInt("tipo")));
		ata.setNumero(rs.getInt("numero"));
		ata.setData(rs.getTimestamp("data"));
		ata.setLocal(rs.getString("local"));
		ata.setLocalCompleto(rs.getString("localCompleto"));
		ata.setDataLimiteComentarios(rs.getDate("dataLimiteComentarios"));
		ata.setConsideracoesIniciais(rs.getString("consideracoesIniciais"));
		ata.setAudio(rs.getBytes("audio"));
		ata.setPublicada(rs.getInt("publicada") == 1);
		ata.setAceitarComentarios(rs.getInt("aceitarComentarios") == 1);
		ata.setDataPublicacao(rs.getTimestamp("dataPublicacao"));
		ata.setDocumento(rs.getBytes("documento"));
		
		return ata;
	}
	
	public boolean temComentarios(int idAta) throws SQLException{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		try{
			conn = ConnectionDAO.getInstance().getConnection();
			stmt = conn.createStatement();
		
			rs = stmt.executeQuery("SELECT COUNT(comentarios.idComentario) AS qtde FROM comentarios " +
				"INNER JOIN pautas ON pautas.idPauta=comentarios.idPauta " + 
				"WHERE pautas.idAta=" + String.valueOf(idAta));
		
			if(rs.next()){
				return (rs.getInt("qtde") > 0);
			}else{
				return false;
			}
		}finally{
			if((rs != null) && !rs.isClosed())
				rs.close();
			if((stmt != null) && !stmt.isClosed())
				stmt.close();
			if((conn != null) && !conn.isClosed())
				conn.close();
		}
	}
	
	public boolean isPresidenteOuSecretario(int idUsuario, int idAta) throws SQLException{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		try{
			conn = ConnectionDAO.getInstance().getConnection();
			stmt = conn.createStatement();
		
			rs = stmt.executeQuery("SELECT atas.idAta FROM atas " +
				"WHERE idAta=" + String.valueOf(idAta) + " AND (idPresidente=" + String.valueOf(idUsuario) + " OR idSecretario=" + String.valueOf(idUsuario) + ")");
		
			return rs.next();
		}finally{
			if((rs != null) && !rs.isClosed())
				rs.close();
			if((stmt != null) && !stmt.isClosed())
				stmt.close();
			if((conn != null) && !conn.isClosed())
				conn.close();
		}
	}
	
	public boolean isPresidente(int idUsuario, int idAta) throws SQLException{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		try{
			conn = ConnectionDAO.getInstance().getConnection();
			stmt = conn.createStatement();
		
			rs = stmt.executeQuery("SELECT atas.idAta FROM atas " +
				"WHERE idAta=" + String.valueOf(idAta) + " AND idPresidente=" + String.valueOf(idUsuario));
		
			return rs.next();
		}finally{
			if((rs != null) && !rs.isClosed())
				rs.close();
			if((stmt != null) && !stmt.isClosed())
				stmt.close();
			if((conn != null) && !conn.isClosed())
				conn.close();
		}
	}

	public boolean isPublicada(int idAta) throws SQLException{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		try{
			conn = ConnectionDAO.getInstance().getConnection();
			stmt = conn.createStatement();
		
			rs = stmt.executeQuery("SELECT atas.publicada FROM atas " +
				"WHERE idAta=" + String.valueOf(idAta));
		
			if(rs.next()) {
				return rs.getInt("publicada") == 1;
			} else {
				return false;
			}
		}finally{
			if((rs != null) && !rs.isClosed())
				rs.close();
			if((stmt != null) && !stmt.isClosed())
				stmt.close();
			if((conn != null) && !conn.isClosed())
				conn.close();
		}
	}


}
